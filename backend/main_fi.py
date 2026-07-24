"""
🚀 FASTAPI AUDIT TRAIL AI - Adapted for Microsoft SQL Server AuditTrail database
Based on the original Sales AI workflow, rewired for Dalet Galaxy Audit Trail.

CORRECTIONS APPLIQUÉES (historique) :
1. Migration complète de PostgreSQL (psycopg2) vers SQL Server (pyodbc).
2. Prompt SQL réécrit en T-SQL, avec le vrai schéma restauré (colonnes/casse observées dans SSMS).
3. Prompt SQL : la règle "ORDER BY Date_Time DESC" n'est plus ajoutée aveuglément.
4. Retry avec backoff exponentiel autour des appels Gemini pour absorber les erreurs
   transitoires 429 RESOURCE_EXHAUSTED / 403 PERMISSION_DENIED (mais PAS les quotas journaliers).
5. Suppression de l'affichage de la clé API complète dans les logs/console (sécurité).
6. Gestion du cas NO_QUERY (salutations / small talk).
7. Authentification (inscription / connexion) avec tables Users / AuthTokens dans SQL Server.
8. sample_size réduit de 200 à 50 lignes pour les demandes de liste (évite le timeout Gemini).
9. Troncature de la colonne ExtraInformation avant envoi au LLM (réduit le volume de tokens).
10. Correction automatique de "IP@Site" -> "[IP@Site]" dans le SQL généré par le LLM.
11. list_prompt enrichi avec labels explicites (ID objet, Type objet, Action, Utilisateur, IP/Site, Date).
12. insights_prompt enrichi pour exploiter ExtraInformation dans le résumé/détails.
13. Timeout LLM porté à 45s.
14. Endpoint /ask/export-pdf : génère un PDF contenant TOUTES les lignes du résultat SQL
    (pas seulement l'échantillon envoyé au LLM), sans passer par Gemini pour la mise en
    forme — donc aucun risque de timeout et aucune limite artificielle sur le nombre de
    lignes affichées (dans la limite de MAX_RESULT_ROWS).
15. Garde-fou anti "SELECT DISTINCT ... ORDER BY <col non présente dans le SELECT>",
    qui provoquait l'erreur SQL Server 145 ("ORDER BY items must appear in the select
    list if SELECT DISTINCT is specified") sur des questions comme "Quels utilisateurs
    ont effectué des actions sur le titre X ?" (le LLM génère souvent un
    SELECT DISTINCT UserLogin, UserDetails ... ORDER BY Date_Time DESC, invalide car
    Date_Time n'est pas dans le SELECT). Le prompt le déconseillait déjà au LLM, mais
    rien ne le garantissait — on corrige donc automatiquement le SQL généré en retirant
    l'ORDER BY fautif plutôt que de laisser la requête planter à l'exécution.
    Appliqué à la fois dans le workflow /ask (node_validate_sql) et dans
    /ask/export-pdf (generate_and_validate_sql), qui partagent tous deux
    SQLValidator.validate().

🆕 NOUVEAUX CORRECTIFS (cette version) :
16. LIST_INTENT_KEYWORDS élargi ("tout les info", "toutes les infos", "tous les
    utilisateurs", "chaque utilisateur", etc.) pour que davantage de formulations
    déclenchent le mode liste (list_prompt) plutôt que le mode résumé (insights_prompt),
    qui tronquait arbitrairement les réponses à quelques exemples.
17. list_prompt rendu adaptatif : n'affiche que les champs réellement présents dans
    chaque ligne de DATA (au lieu d'un gabarit figé "ID objet / Type objet / Action /
    Utilisateur / IP/Site / Date" qui laissait des labels vides pour les requêtes
    agrégées de type "utilisateurs les plus connectés", où seules les colonnes
    UserDetails + count existent réellement).
18. sql_prompt enrichi d'une règle "RANKING + DETAILS PATTERN" : quand la question
    combine un classement ("les plus connectés", "top utilisateurs") ET une demande
    de détail ("toutes les informations", "tous les détails"), le LLM génère désormais
    une requête en deux temps (sous-requête TOP N + SELECT détaillé filtré sur ces
    utilisateurs) au lieu d'une simple agrégation GROUP BY qui perdrait irrémédiablement
    IP@Site / Date_Time / ExtraInformation (une agrégation ne peut, par construction,
    conserver le détail de chaque ligne individuelle).
19. format_row_for_pdf() rendu dynamique côté export PDF (même logique que le point 17,
    mais en Python pur puisque cette fonction ne passe pas par le LLM) : les colonnes
    connues (ID objet, Type objet, Action, Utilisateur, IP/Site, Date) ne s'affichent
    que si elles existent réellement dans la ligne, et toute colonne supplémentaire
    (ex: un total ou un compteur issu d'une requête agrégée) est affichée avec un label
    lisible dérivé de son nom de colonne, au lieu d'être silencieusement ignorée.
"""

import os
import re
import io
import time
import uuid
import asyncio
import json
import traceback
import hashlib
import secrets
from typing import Dict, Any, List, Optional, TypedDict, AsyncGenerator
from datetime import datetime, date
from contextlib import contextmanager

import pyodbc

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse, Response
from pydantic import BaseModel, Field

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate

from langgraph.graph import StateGraph, END

from dotenv import load_dotenv

# reportlab pour la génération de PDF (pip install reportlab)
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from xml.sax.saxutils import escape as xml_escape

load_dotenv()

_loaded_key = os.getenv("GOOGLE_API_KEY", "")
print("loaded env:", bool(_loaded_key))
print("key prefix:", (_loaded_key[:8] + "...") if _loaded_key else "MISSING")

# ============================================================
# Configuration
# ============================================================
SQL_SERVER = os.getenv("SQL_SERVER", r"localhost\SQLEXPRESS")
SQL_DB = os.getenv("SQL_DB", "SNRTAuditTrail")
SQL_DRIVER = os.getenv("SQL_DRIVER", "{ODBC Driver 18 for SQL Server}")
SQL_USER = os.getenv("SQL_USER", "")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "")
SQL_USE_WINDOWS_AUTH = os.getenv("SQL_USE_WINDOWS_AUTH", "true").lower() == "true"

MAX_RESULT_ROWS = 10000
LOG_DIR = "logs"
LLM_TIMEOUT = 45

LLM_MAX_RETRIES = 4
LLM_BASE_DELAY = 2.0

LIST_SAMPLE_SIZE = 50
SUMMARY_SAMPLE_SIZE = 20
EXTRA_INFO_MAX_LEN = 250

# Nombre max de lignes affichées dans un export PDF (garde-fou anti-abus)
PDF_MAX_ROWS = 2000

os.makedirs(LOG_DIR, exist_ok=True)


# ============================================================
# State
# ============================================================
class ProcessingState(TypedDict, total=False):
    question: str
    session_id: str
    voice_mode: bool
    sql_query: Optional[str]
    query_results: Optional[Dict[str, Any]]
    row_count: int
    response_text: str
    errors: List[str]
    steps_completed: List[str]
    metadata: Dict[str, Any]
    request_id: str


# ============================================================
# Logger
# ============================================================
class Logger:
    @staticmethod
    def log(msg: str, level: str = "INFO", request_id: str = ""):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        req_prefix = f"[{request_id[:8]}]" if request_id else ""
        log_entry = f"[{timestamp}] [{level}] {req_prefix} {msg}"
        print(log_entry)
        try:
            with open(f"{LOG_DIR}/app.log", "a", encoding="utf-8") as f:
                f.write(log_entry + "\n")
        except Exception:
            pass


logger = Logger()


# ============================================================
# Database Manager
# ============================================================
class DatabaseManager:
    @staticmethod
    def create_session(voice_mode: bool = False, user_id: Optional[int] = None) -> str:
        session_id = str(uuid.uuid4())
        now = datetime.now()
        with DatabaseManager.get_sql_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO dbo.ChatSessions (session_id, user_id, created_at, updated_at, voice_mode)
                VALUES (?, ?, ?, ?, ?)
            """, (session_id, user_id, now, now, 1 if voice_mode else 0))
            conn.commit()
        logger.log(f"Session created: {session_id[:8]}...")
        return session_id

    @staticmethod
    def list_sessions(user_id: int, limit: int = 30) -> List[Dict]:
        try:
            with DatabaseManager.get_sql_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT TOP (?) session_id, updated_at
                    FROM dbo.ChatSessions
                    WHERE user_id = ?
                    ORDER BY updated_at DESC
                """, (limit, user_id))
                sessions = cursor.fetchall()

                result = []
                for row in sessions:
                    session_id = row[0]
                    cursor.execute("""
                        SELECT TOP 1 content FROM dbo.ChatHistory
                        WHERE session_id = ? AND role = 'user'
                        ORDER BY id ASC
                    """, (session_id,))
                    first_msg = cursor.fetchone()
                    title = first_msg[0] if first_msg else "Nouvelle conversation"
                    if len(title) > 40:
                        title = title[:40] + "..."
                    result.append({
                        "session_id": str(session_id),
                        "title": title,
                        "updated_at": row[1].isoformat() if row[1] else None
                    })
                return result
        except Exception as e:
            logger.log(f"list_sessions error: {str(e)}", "ERROR")
            return []

    @staticmethod
    def save_message(session_id: str, role: str, content: str):
        try:
            with DatabaseManager.get_sql_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO dbo.ChatHistory (session_id, role, content, timestamp)
                    VALUES (?, ?, ?, ?)
                """, (session_id, role, content, datetime.now()))
                cursor.execute("""
                    UPDATE dbo.ChatSessions SET updated_at = ? WHERE session_id = ?
                """, (datetime.now(), session_id))
                conn.commit()
        except Exception as e:
            logger.log(f"Save message error: {str(e)}", "ERROR")

    @staticmethod
    def get_history(session_id: str, limit: int = 10) -> List[Dict]:
        try:
            with DatabaseManager.get_sql_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT TOP (?) role, content, timestamp FROM dbo.ChatHistory
                    WHERE session_id = ?
                    ORDER BY id DESC
                """, (limit, session_id))
                rows = cursor.fetchall()
            return [
                {"role": row[0], "content": row[1], "timestamp": row[2].isoformat() if row[2] else None}
                for row in reversed(rows)
            ]
        except Exception as e:
            logger.log(f"Get history error: {str(e)}", "ERROR")
            return []

    @staticmethod
    def delete_session(session_id: str):
        try:
            with DatabaseManager.get_sql_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM dbo.ChatHistory WHERE session_id = ?", (session_id,))
                cursor.execute("DELETE FROM dbo.ChatSessions WHERE session_id = ?", (session_id,))
                conn.commit()
        except Exception as e:
            logger.log(f"Delete session error: {str(e)}", "ERROR")

    @staticmethod
    @contextmanager
    def get_sql_connection():
        conn = None
        try:
            if SQL_USE_WINDOWS_AUTH:
                conn_str = (
                    f"DRIVER={SQL_DRIVER};"
                    f"SERVER={SQL_SERVER};"
                    f"DATABASE={SQL_DB};"
                    "Trusted_Connection=yes;"
                    "TrustServerCertificate=yes;"
                )
            else:
                conn_str = (
                    f"DRIVER={SQL_DRIVER};"
                    f"SERVER={SQL_SERVER};"
                    f"DATABASE={SQL_DB};"
                    f"UID={SQL_USER};"
                    f"PWD={SQL_PASSWORD};"
                    "TrustServerCertificate=yes;"
                )
            conn = pyodbc.connect(conn_str, timeout=10)
            yield conn
        except Exception as e:
            logger.log(f"SQL Server connection error: {str(e)}", "ERROR")
            raise e
        finally:
            if conn:
                conn.close()


# ============================================================
# Authentification — Users / AuthTokens stockés dans SQL Server
# ============================================================
#
# CREATE TABLE dbo.Users (
#     id INT IDENTITY(1,1) PRIMARY KEY,
#     username NVARCHAR(50) NOT NULL UNIQUE,
#     password_hash NVARCHAR(256) NOT NULL,
#     salt NVARCHAR(64) NOT NULL,
#     created_at DATETIME DEFAULT GETDATE()
# );
#
# CREATE TABLE dbo.AuthTokens (
#     token NVARCHAR(64) PRIMARY KEY,
#     user_id INT NOT NULL,
#     created_at DATETIME DEFAULT GETDATE(),
#     FOREIGN KEY (user_id) REFERENCES dbo.Users(id)
# );
#
# CREATE TABLE dbo.ChatSessions (
#     session_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
#     user_id INT NOT NULL,
#     created_at DATETIME DEFAULT GETDATE(),
#     updated_at DATETIME DEFAULT GETDATE(),
#     voice_mode BIT DEFAULT 0,
#     FOREIGN KEY (user_id) REFERENCES dbo.Users(id)
# );
#
# CREATE TABLE dbo.ChatHistory (
#     id INT IDENTITY(1,1) PRIMARY KEY,
#     session_id UNIQUEIDENTIFIER NOT NULL,
#     role NVARCHAR(20) NOT NULL,
#     content NVARCHAR(MAX) NOT NULL,
#     timestamp DATETIME DEFAULT GETDATE(),
#     FOREIGN KEY (session_id) REFERENCES dbo.ChatSessions(session_id)
# );
#
# ============================================================

def hash_password(password: str, salt: str = None) -> tuple:
    if salt is None:
        salt = secrets.token_hex(16)
    pwd_hash = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("utf-8"), 100_000
    ).hex()
    return pwd_hash, salt


def verify_password(password: str, stored_hash: str, salt: str) -> bool:
    computed_hash, _ = hash_password(password, salt)
    return secrets.compare_digest(computed_hash, stored_hash)


class AuthManager:
    @staticmethod
    def create_user(username: str, password: str) -> Optional[int]:
        pwd_hash, salt = hash_password(password)
        try:
            with DatabaseManager.get_sql_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO dbo.Users (username, password_hash, salt, created_at)
                    VALUES (?, ?, ?, ?)
                """, (username, pwd_hash, salt, datetime.now()))
                conn.commit()

                cursor.execute("SELECT id FROM dbo.Users WHERE username = ?", (username,))
                row = cursor.fetchone()
                return row[0] if row else None
        except pyodbc.IntegrityError:
            logger.log(f"create_user: username already exists ({username})", "WARNING")
            return None
        except Exception as e:
            logger.log(f"create_user error: {str(e)}", "ERROR")
            return None

    @staticmethod
    def authenticate(username: str, password: str) -> Optional[int]:
        try:
            with DatabaseManager.get_sql_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id, password_hash, salt FROM dbo.Users WHERE username = ?",
                    (username,)
                )
                row = cursor.fetchone()
                if not row:
                    return None
                user_id, stored_hash, salt = row
                if verify_password(password, stored_hash, salt):
                    return user_id
                return None
        except Exception as e:
            logger.log(f"authenticate error: {str(e)}", "ERROR")
            return None

    @staticmethod
    def create_token(user_id: int) -> str:
        token = secrets.token_hex(32)
        with DatabaseManager.get_sql_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO dbo.AuthTokens (token, user_id, created_at)
                VALUES (?, ?, ?)
            """, (token, user_id, datetime.now()))
            conn.commit()
        return token

    @staticmethod
    def get_user_from_token(token: str) -> Optional[int]:
        try:
            with DatabaseManager.get_sql_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id FROM dbo.AuthTokens WHERE token = ?", (token,))
                row = cursor.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.log(f"get_user_from_token error: {str(e)}", "ERROR")
            return None


def get_current_user(authorization: str = Header(None)) -> int:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Non authentifié")
    token = authorization.replace("Bearer ", "").strip()
    user_id = AuthManager.get_user_from_token(token)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Token invalide ou expiré")
    return user_id


# ============================================================
# LLM
# ============================================================
try:
    llm = ChatGoogleGenerativeAI(
        model="gemini-flash-lite-latest",
        google_api_key=os.getenv("GOOGLE_API_KEY"),
        temperature=0,
        timeout=LLM_TIMEOUT,
    )
    logger.log("✅ LLM initialized successfully")
except Exception as e:
    logger.log(f"❌ LLM initialization failed: {str(e)}", "ERROR")
    raise


def invoke_llm_with_retry(llm_instance, prompt: str, request_id: str = ""):
    transient_markers = (
        "RESOURCE_EXHAUSTED",
        "PERMISSION_DENIED",
        "UNAVAILABLE",
        "429",
        "403",
        "503",
    )
    non_retryable_quota_markers = ("PerDay",)

    last_exc = None
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        try:
            return llm_instance.invoke(prompt)
        except Exception as e:
            last_exc = e
            err_str = str(e)

            if any(marker in err_str for marker in non_retryable_quota_markers):
                logger.log(
                    "Daily quota exceeded — no retry, failing fast",
                    "ERROR",
                    request_id,
                )
                raise

            is_transient = any(marker in err_str for marker in transient_markers)

            if not is_transient or attempt == LLM_MAX_RETRIES:
                raise

            delay = LLM_BASE_DELAY * (2 ** (attempt - 1))
            logger.log(
                f"LLM call failed (attempt {attempt}/{LLM_MAX_RETRIES}), "
                f"retrying in {delay:.1f}s: {err_str[:150]}",
                "WARNING",
                request_id,
            )
            time.sleep(delay)

    raise last_exc


# ============================================================
# Input Sanitizer
# ============================================================
class InputSanitizer:
    @staticmethod
    def sanitize_question(question: str) -> str:
        question = " ".join(question.split())

        dangerous_patterns = [
            r"ignore\s+previous\s+instructions",
            r"disregard\s+above",
            r"you\s+are\s+now",
            r"new\s+instructions",
        ]

        for pattern in dangerous_patterns:
            question = re.sub(pattern, "", question, flags=re.IGNORECASE)

        return question.strip()


# ============================================================
# Post-traitement du SQL généré : corrige [IP@Site] si le LLM a oublié les crochets
# ============================================================
def fix_ip_site_column(sql: str) -> str:
    """Remplace toute occurrence de IP@Site non déjà entourée de crochets par [IP@Site]."""
    return re.sub(r'(?<!\[)\bIP@Site\b(?!\])', '[IP@Site]', sql, flags=re.IGNORECASE)


# ============================================================
# 🆕 Post-traitement du SQL généré : retire un ORDER BY invalide en présence
# de SELECT DISTINCT quand la colonne triée n'apparaît pas dans le SELECT.
# En T-SQL, "SELECT DISTINCT colA, colB ... ORDER BY colC" lève l'erreur 145
# ("ORDER BY items must appear in the select list if SELECT DISTINCT is
# specified") si colC n'est pas dans le SELECT. Le prompt demande déjà au LLM
# d'éviter ce cas, mais un LLM peut toujours l'ignorer — on corrige donc le
# SQL après génération plutôt que de laisser la requête planter à l'exécution.
# ============================================================
def strip_invalid_order_by_for_distinct(sql: str, request_id: str = "") -> str:
    sql_lower = sql.lower()
    if 'select distinct' not in sql_lower:
        return sql

    order_match = re.search(r'\border\s+by\s+(.+?)$', sql, re.IGNORECASE | re.DOTALL)
    if not order_match:
        return sql

    # Isole la clause SELECT ... avant le premier "FROM" (au niveau racine)
    select_clause = sql_lower.split(' from ')[0]

    order_cols = [
        c.strip().split()[0].lower().lstrip('[').rstrip(']')
        for c in order_match.group(1).split(',')
        if c.strip()
    ]

    missing = [col for col in order_cols if col not in select_clause]
    if missing:
        new_sql = re.sub(r'\s+order\s+by\s+.+$', '', sql, flags=re.IGNORECASE | re.DOTALL).strip()
        logger.log(
            f"Removed invalid ORDER BY on SELECT DISTINCT query "
            f"(columns not in SELECT: {missing})",
            "WARNING",
            request_id,
        )
        return new_sql

    return sql


# ============================================================
# Troncature d'ExtraInformation avant envoi au LLM (réduit le volume de tokens)
# ============================================================
def truncate_extra_info(data_sample: List[Dict], max_len: int = EXTRA_INFO_MAX_LEN) -> List[Dict]:
    for row in data_sample:
        val = row.get("ExtraInformation")
        if isinstance(val, str) and len(val) > max_len:
            row["ExtraInformation"] = val[:max_len] + "…"
    return data_sample


# ============================================================
# Reformulation d'ExtraInformation en phrase lisible (sans LLM)
# Reproduit en Python ce que Gemini faisait dans le chat, en extrayant les
# balises <hostName> et <Name> du XML et en les insérant dans un gabarit de
# phrase selon le type d'action. Beaucoup plus rapide et fiable qu'un appel
# LLM par ligne (qui ferait exploser le temps de génération du PDF).
# ============================================================
ACTION_VERB_TEMPLATES = {
    "RUNDOWN_INSERT_ITEMS": "Insertion des items du rundown {name} sur le host {host}.",
    "RUNDOWN_REMOVE_ITEMS": "Suppression des items du rundown {name} sur le host {host}.",
    "RUNDOWN_UPDATE_ITEMS": "Mise à jour des items du rundown {name} sur le host {host}.",
    "RUNDOWN_MOVE_ITEMS": "Déplacement des items du rundown {name} sur le host {host}.",
    "CREATE_TITLE": "Création du titre {name} sur le host {host}.",
    "DELETE_TITLE": "Suppression du titre {name} sur le host {host}.",
    "MODIFY_TITLE_METADATA": "Modification des métadonnées du titre {name} sur le host {host}.",
    "MODIFY_TITLE_MEDIA": "Modification du média du titre {name} sur le host {host}.",
    "MODIFY_TITLE_STATUS": "Modification du statut du titre {name} sur le host {host}.",
    "MOVE_TITLE": "Déplacement du titre {name} sur le host {host}.",
    "RESTORE_TITLE": "Restauration du titre {name} sur le host {host}.",
    "USER_LOGIN": "Connexion de l'utilisateur sur le host {host}.",
    "USER_LOGOUT": "Déconnexion de l'utilisateur sur le host {host}.",
}

_HOSTNAME_RE = re.compile(r'<hostName[^>]*>([^<]*)</hostName>')
_NAME_RE = re.compile(r'<Name[^>]*>([^<]*)</Name>')


def build_readable_detail(action_type: str, extra_info: str) -> str:
    """Construit une phrase lisible à partir du XML brut d'ExtraInformation,
    en se basant sur le type d'action. Retombe sur une phrase générique si
    le type d'action n'a pas de gabarit dédié, ou sur 'aucun détail' si
    ExtraInformation est vide."""
    if not extra_info:
        return "aucun détail"

    host_match = _HOSTNAME_RE.search(extra_info)
    name_match = _NAME_RE.search(extra_info)
    host = host_match.group(1).strip() if host_match else None
    name = name_match.group(1).strip() if name_match else None

    template = ACTION_VERB_TEMPLATES.get(action_type)
    if template:
        if "{name}" in template and "{host}" in template:
            if name and host:
                return template.format(name=name, host=host)
            elif host:
                # Gabarit attend un nom mais on n'en a pas trouvé
                return template.replace("{name} ", "").format(host=host)
        elif "{host}" in template and host:
            return template.format(host=host)

    # Fallback générique : reformule le nom de l'action (ex: "MODIFY_TITLE_MEDIA"
    # -> "Modify title media") et ajoute host/name si disponibles
    generic_action = action_type.replace("_", " ").capitalize() if action_type else "Action"
    parts = [generic_action]
    if name:
        parts.append(f"sur {name}")
    if host:
        parts.append(f"(host {host})")
    return " ".join(parts) + "."


# ============================================================
# Extraction "tolérante" des champs logiques d'une ligne SQL, quel que soit
# l'alias exact utilisé par le LLM (ex: [IP@Site] vs "MachineIp", UserDetails vs UserLogin...)
# ============================================================
FIELD_ALIASES = {
    "object_id": ["ActionObjectID", "ActionObjectId", "TitleId", "ObjectID"],
    "object_type": ["ActionObjectType", "ObjectType"],
    "action_type": ["ActionType", "Action"],
    "user": ["UserDetails", "UserLogin", "UserName", "Utilisateur"],
    "ip_site": ["IP@Site", "[IP@Site]", "MachineIp", "IPSite", "Site"],
    "date_time": ["Date_Time", "TimeOfOperation", "Date"],
    "extra_info": ["ExtraInformation", "Detail", "Details"],
}

# Ordre d'affichage + libellés français des champs "connus"
_KNOWN_FIELD_LABELS = [
    ("object_id", "ID objet"),
    ("object_type", "Type objet"),
    ("action_type", "Action"),
    ("user", "Utilisateur"),
    ("ip_site", "IP/Site"),
    ("date_time", "Date"),
]


def get_field(row: Dict, field_key: str) -> str:
    """Retourne la valeur du premier alias trouvé dans row pour ce champ logique,
    ou une chaîne vide si aucun des alias possibles n'est présent."""
    for alias in FIELD_ALIASES.get(field_key, []):
        if alias in row and row[alias] is not None:
            return str(row[alias])
    return ""


def _humanize_column_name(col: str) -> str:
    """Transforme un nom de colonne SQL (souvent un alias choisi librement par le LLM,
    ex: 'ConnexionCount', 'nb_connexions') en libellé lisible pour l'affichage PDF."""
    # Sépare camelCase -> mots, puis remplace les underscores par des espaces
    spaced = re.sub(r'(?<!^)(?=[A-Z])', ' ', col).replace('_', ' ').strip()
    spaced = re.sub(r'\s+', ' ', spaced)
    return spaced[:1].upper() + spaced[1:] if spaced else col


def format_row_for_pdf(row: Dict, extra_info_max_len: int = 300) -> str:
    """🆕 Formate une ligne SQL de façon DYNAMIQUE : n'affiche que les champs
    (ID objet, Type objet, Action, Utilisateur, IP/Site, Date) réellement présents
    dans la ligne, et affiche en plus toute colonne non standard (ex: un compteur
    ou un total issu d'une requête agrégée type GROUP BY) avec un libellé lisible
    dérivé de son nom de colonne. Auparavant, ces champs "inconnus" pour le format
    étaient purement ignorés (cas des requêtes de classement/agrégation), ce qui
    donnait des lignes avec des labels vides."""
    matched_aliases = set()
    header_parts = []

    for field_key, label in _KNOWN_FIELD_LABELS:
        value = None
        for alias in FIELD_ALIASES.get(field_key, []):
            if alias in row and row[alias] is not None and str(row[alias]).strip() != "":
                value = str(row[alias])
                matched_aliases.add(alias)
                break
        if value:
            header_parts.append(f"<b>{label}:</b> {xml_escape(value)}")

    # Marque les alias d'ExtraInformation comme "consommés" (traités séparément ci-dessous)
    for alias in FIELD_ALIASES.get("extra_info", []):
        matched_aliases.add(alias)

    # 🆕 Colonnes supplémentaires non standard (ex: résultat d'une requête agrégée
    # comme COUNT(*) AS NombreConnexions) : affichées avec un libellé dérivé du nom
    for col, val in row.items():
        if col in matched_aliases:
            continue
        if val is None or str(val).strip() == "":
            continue
        label = _humanize_column_name(col)
        header_parts.append(f"<b>{label}:</b> {xml_escape(str(val))}")

    header = " | ".join(header_parts) if header_parts else "(aucune information disponible)"

    extra_info_raw = get_field(row, "extra_info")
    if extra_info_raw:
        action_type = get_field(row, "action_type")
        detail_text = build_readable_detail(action_type, extra_info_raw)
        detail = f"&nbsp;&nbsp;&nbsp;Détail : {xml_escape(detail_text)}"
        return f"• {header}<br/>{detail}"

    return f"• {header}"


# ============================================================
# Génération du SQL à partir d'une question, réutilisable par /ask et /ask/export-pdf
# ============================================================
def generate_and_validate_sql(question: str, request_id: str) -> Optional[str]:
    """Retourne le SQL validé, ou None si la question ne demande pas de données (NO_QUERY)."""
    clean_question = InputSanitizer.sanitize_question(question)
    prompt = sql_prompt.format(question=clean_question)
    response = invoke_llm_with_retry(llm, prompt, request_id)
    sql = response.text.strip()
    sql = re.sub(r'```sql\s*', '', sql)
    sql = re.sub(r'```\s*', '', sql).strip()

    if not sql:
        raise ValueError("Generated SQL is empty")

    if sql.startswith("NO_QUERY:"):
        return None

    sql = fix_ip_site_column(sql)
    sql = strip_invalid_order_by_for_distinct(sql, request_id)  # 🆕
    sql = SQLValidator.validate(sql)
    return sql


# ============================================================
# Prompts — wired to the real AuditTrail schema (SQL Server / T-SQL)
# ============================================================
ACTION_TYPE_MAP = """
-1 UNKNOWN
0  USER_LOGIN
1  USER_LOGOUT
2  CREATE_TITLE
3  CREATE_TITLE_BY_INGEST_SERVER
4  CREATE_TITLE_BY_MIRROR_SERVER
5  CREATE_TITLE_VERSION
6  MODIFY_TITLE_STATUS
7  DELETE_TITLE
8  RESTORE_TITLE
9  PURGE_ITEM
10 MOVE_TITLE
11 CREATE_TITLE_LINK
12 DELETE_TITLE_LINK
13 MODIFY_TITLE_METADATA
14 MODIFY_TITLE_MEDIA
15 PURGE_TITLE_MEDIA
16 CREATE_CATEGORY
17 DELETE_CATEGORY
18 RESTORE_CATEGORY
19 PURGE_CATEGORY
20 MODIFY_CATEGORY
21 EMPTY_RECYCLE_BIN
22 JOB_CREATED
23 JOB_STARTED
24 JOB_COMPLETED
25 JOB_ABORTED
26 JOB_ERROR
27 RESTRICTED_MEDIA_USED
28 PREVIEW_TITLE
29 DOWNLOAD_TITLE
30 METADATA_IMPORT
31 METADATA_IMPORT_ERROR
32 DELETE_CLOCK
33 EXPORT_TO_NLE
34 SEARCH_QUERY
35 SO_ME_POPULARITY_DURATION
36 SO_ME_POPULARITY_DATA
37 RUNDOWN_INSERT_ITEMS
38 RUNDOWN_MOVE_ITEMS
39 RUNDOWN_UPDATE_ITEMS
40 RUNDOWN_REMOVE_ITEMS
"""

# 🆕 Règle "classement + détails" : voir point 18 de l'en-tête du fichier.
RANKING_WITH_DETAILS_RULE = """
- RANKING + DETAILS PATTERN: If the question asks to RANK or find the MOST/LEAST active
  users/objects (e.g. "les plus connectés", "top utilisateurs", "utilisateurs les plus actifs")
  AND ALSO asks for details/information about them (e.g. "toutes les informations",
  "tous les détails", "chaque connexion", "avec IP et date") — do NOT return a single
  aggregated row per user (which would lose IP@Site, Date_Time, ExtraInformation, since
  GROUP BY collapses multiple rows into one). Instead, generate a query that:
  1. Uses a subquery to identify the TOP N users/objects by count (one level of nesting only)
  2. Selects the DETAILED rows (ActionObjectID, ActionObjectType, ActionType, UserDetails,
     [IP@Site], Date_Time, ExtraInformation) for only those top N users/objects
  3. Orders the final result by Date_Time DESC (or by user then date)

  Example, for "toutes les informations sur les utilisateurs les plus connectés ce mois-ci":

  SELECT UserDetails, [IP@Site], Date_Time, ActionType, ExtraInformation
  FROM dbo.AuditTrail
  WHERE ActionType = 'USER_LOGIN'
    AND MONTH(Date_Time) = MONTH(GETDATE()) AND YEAR(Date_Time) = YEAR(GETDATE())
    AND UserDetails IN (
      SELECT TOP 10 UserDetails FROM dbo.AuditTrail
      WHERE ActionType = 'USER_LOGIN'
        AND MONTH(Date_Time) = MONTH(GETDATE()) AND YEAR(Date_Time) = YEAR(GETDATE())
      GROUP BY UserDetails
      ORDER BY COUNT(*) DESC
    )
  ORDER BY UserDetails, Date_Time DESC

  If the question asks ONLY for the ranking/counts (e.g. "qui sont les utilisateurs les plus
  connectés", with no mention of details), the simple aggregated GROUP BY + COUNT query
  remains correct and preferred — do not over-complicate when details were not requested.
"""

sql_prompt = PromptTemplate(
    input_variables=["question"],
    template=f"""You are a Microsoft SQL Server (T-SQL) expert. Convert questions into a single T-SQL SELECT query.
You do not know the schema in advance — you MUST rely only on the schema and rules given below.

TABLE: dbo.AuditTrail
COLUMNS:
- ActionId          (nvarchar(64), primary key)
- UserLogin         (nvarchar(128)) - login of the user who performed the action
- UserDetails       (nvarchar(128)) - full name of the user
- [IP@Site]         (nvarchar(128)) - machine/site from which the action was performed. MUST always be wrapped in square brackets in SQL, because of the "@" character (e.g. SELECT [IP@Site] FROM dbo.AuditTrail).
- Application       (nvarchar(128)) - application that triggered the action (e.g. DaletGalaxy, JobBroker)
- Date_Time         (datetime) - when the action occurred
- ActionType        (nvarchar(128)) - the TEXTUAL LABEL identifying the type of action (e.g. 'MODIFY_TITLE_MEDIA', 'USER_LOGIN', 'DELETE_TITLE'). It is stored as the label itself, NOT as a numeric code. See the reference table below for the full list of valid labels and their meaning.
- ActionObjectID    (nvarchar(1024)) - id of the object impacted (title id, job id, username, etc.)
- ActionObjectType  (nvarchar(128)) - type of object impacted (TITLE, USER, JOB, CATEGORY, CLOCK, ...)
- ExtraInformation  (nvarchar(max)) - additional details about the action, often XML-like text

ACTIONTYPE REFERENCE (code -> label — the label is what is actually stored in the ActionType column):
{ACTION_TYPE_MAP}

RULES:
- ONLY SELECT queries (security) — never produce INSERT, UPDATE, DELETE, DROP, etc.
- This is T-SQL (SQL Server), NOT PostgreSQL:
  - Use "SELECT TOP N ..." instead of "... LIMIT N" for "top N" style requests
  - Use "LIKE" for case-insensitive text search (SQL Server's default collation is already case-insensitive), NOT "ILIKE"
  - Do NOT use "::text" casts (PostgreSQL-only syntax) — ExtraInformation is already nvarchar, use it directly with LIKE
  - Always wrap the column [IP@Site] in square brackets since it contains "@"
- To filter by a named action (e.g. "logins", "deleted titles", "modification de média"), find the matching label(s) in the reference table above and filter using ActionType = '<LABEL>' (e.g. ActionType = 'MODIFY_TITLE_MEDIA'). NEVER use the numeric code in the SQL — the column contains the text label, not the number.
- Use GROUP BY, ORDER BY as needed
- NO subqueries beyond one level, NO UNION statements
- NO comments (-- or /* */)
- Whenever the question relates to a specific action's details, modifications, or context, ALWAYS include the ExtraInformation column in the SELECT, even if not explicitly asked, since it often contains the most relevant details.
- If no explicit ordering is requested, add "ORDER BY Date_Time DESC" ONLY when it is valid T-SQL:
  - NEVER add it to a query using SELECT DISTINCT unless Date_Time is also in the SELECT list
  - NEVER add it to a query using aggregate functions (COUNT, SUM, AVG, MAX, MIN) without a matching GROUP BY that includes Date_Time
  - For aggregate-only queries (e.g. COUNT(*), COUNT(DISTINCT col)) with no GROUP BY, do NOT add any ORDER BY at all
  - For GROUP BY queries, you may order by the aggregate or grouped column instead, e.g. "ORDER BY COUNT(*) DESC"
{RANKING_WITH_DETAILS_RULE}
IMPORTANT — NON-DATA QUESTIONS:
If the USER QUESTION is NOT a request for data from the AuditTrail table — for example a greeting ("bonjour", "hello"), thanks, small talk, or a question totally unrelated to audit/user/action data — do NOT invent a SQL query. Instead, respond with EXACTLY this format (nothing else):
NO_QUERY: <a short, friendly reply in the same language as the question, briefly explaining what you can help with, e.g. asking about logins, deleted titles, user activity, etc.>

USER QUESTION: {{question}}

Return ONLY the SQL query, no markdown, no explanation:"""
)

insights_prompt = PromptTemplate(
    input_variables=["question", "data", "row_count"],
    template="""You are an assistant analyzing Dalet Galaxy Audit Trail data (a security/activity log).

QUESTION: {question}
RESULTS: {row_count} rows
DATA: {data}

IMPORTANT: Detect the language of the QUESTION above and respond in that exact same language. If the question is in French, respond entirely in French. If the question is in English, respond entirely in English. Do not mix languages.

If a row contains an ExtraInformation field, examine its content carefully (it is often
XML-like text, possibly truncated) and extract any meaningful detail from it (e.g. old/new
values, affected fields, error codes, source system) to enrich your explanation of that row.

FORMAT STRICT — follow this exact structure, using plain text only (NO markdown: no **, no ##, no numbered lists with dots):

📊 Résumé
<1-2 short sentences: how many results, over what period/scope>

🔍 Détails
<3-5 short bullet-style lines, each starting with "• ", describing who did what, when, and any notable pattern (repeated errors, unusual deletions, off-hours activity, etc.), including relevant details from ExtraInformation when present>

⚠️ Signalement
<1-2 sentences: flag anything suspicious, OR state clearly that nothing unusual was found — pick one emoji prefix depending on the case: "✅ Rien d'inhabituel détecté." or "⚠️ Point à vérifier : ...">

Be concise, factual, professional, and do not add any other section or markdown symbol beyond what is specified above."""
)

LIST_INTENT_KEYWORDS = [
    "liste", "lister", "listez", "listes", "énumère", "enumere", "énumérer", "enumerer",
    "donne la liste", "donnez la liste", "donner la liste", "montre tous", "montrez tous",
    "montre toutes", "montrez toutes", "affiche tous", "affichez tous", "affiche toutes",
    "affichez toutes", "quels sont les titres", "quelles sont les titres", "tous les titres",
    "toutes les actions", "détail par titre", "detail par titre", "détail de chaque",
    "list", "list all", "show all", "show me all", "enumerate", "give me the list",
    "give me a list", "which titles", "what are the titles", "tout les info", "toutes les infos",
    "tout les infos", "toutes les informations",
    "tous les info", "toute les information", "donner tout", "donne tout", "donnez tout",
    "toutes les données", "tout le détail", "tous les détails", "tous les utilisateurs",
    "toutes les utilisateurs", "chaque utilisateur", "détail de chaque utilisateur",
    "détail complet", "tous les résultats", "toutes les lignes", "give me all",
    "all details", "full details", "every user", "each user",
]


def is_list_request(question: str) -> bool:
    q = (question or "").lower()
    return any(keyword in q for keyword in LIST_INTENT_KEYWORDS)


list_prompt = PromptTemplate(
    input_variables=["question", "data", "row_count"],
    template="""You are an assistant analyzing Dalet Galaxy Audit Trail data (a security/activity log).

QUESTION: {question}
RESULTS: {row_count} rows
DATA: {data}

IMPORTANT: Detect the language of the QUESTION above and respond in that exact same language.

The user explicitly asked for a LIST / enumeration of results. You MUST list EVERY single row
present in DATA individually — do NOT summarize, do NOT group further, do NOT limit yourself to
a few examples. Every row in DATA must appear as its own line in your answer.

CRITICAL — DATA CAN HAVE DIFFERENT SHAPES:
- If the question is about raw audit events, each row typically has fields like
  ActionObjectID, ActionObjectType, ActionType, UserDetails/UserLogin, IP@Site, Date_Time,
  ExtraInformation.
- If the question is about an AGGREGATION (e.g. "top users by number of connections",
  "most active users", counts, totals), each row will instead have ONLY a few fields such
  as a user name and a count/total (whatever column names the SQL query actually produced).

YOU MUST ONLY DISPLAY FIELDS THAT ACTUALLY EXIST AND HAVE A VALUE IN EACH ROW OF DATA.
NEVER print a fixed set of labels if the corresponding field is absent from the row — leaving
labels empty (e.g. "IP/Site: ") is FORBIDDEN. Only include a label if you have a real value
for it in that specific row.

FORMAT STRICT — plain text only (NO markdown: no **, no ##):

📋 Liste ({row_count} résultat(s))
<one line per row: "• " followed by "Label: Value" pairs, separated by " | ", built ONLY from
the fields actually present in that row of DATA. Use these French labels when the matching
column is present:
  ActionObjectID -> "ID objet", ActionObjectType -> "Type objet", ActionType -> "Action",
  UserDetails or UserLogin -> "Utilisateur", IP@Site or MachineIp -> "IP/Site",
  Date_Time -> "Date", ExtraInformation -> use it only to build a short "Détail" line
  (max 1 sentence), never print it raw.
For any OTHER column not in this list (e.g. a count, a total, an aggregate alias), use a
clear, human-readable French label describing what the number/value represents (e.g.
"Nombre de connexions", "Total"), based on the column name and the QUESTION's context.
Do not add a "Détail" line for rows that have no ExtraInformation field at all — simply
omit it rather than writing "Détail : aucun détail".>

If DATA is empty, simply state that no results were found — do not invent data.

Be factual and do not add any other section or markdown symbol beyond what is specified above."""
)


# ============================================================
# SQL Validator
# ============================================================
class SQLValidator:
    @staticmethod
    def validate(sql: str) -> str:
        sql_clean = sql.strip().rstrip(';').strip()
        sql_lower = sql_clean.lower()

        if not sql_lower.startswith('select'):
            raise ValueError("Only SELECT queries allowed")

        if ';' in sql_clean:
            raise ValueError("Multi-statement queries not allowed")

        forbidden = [
            'insert', 'update', 'delete', 'drop', 'alter',
            'truncate', 'create', 'replace', 'exec', 'execute',
            'union', 'into', 'grant', 'revoke', 'merge',
        ]
        for keyword in forbidden:
            if re.search(r'\b' + keyword + r'\b', sql_lower):
                raise ValueError(f"Forbidden keyword: {keyword.upper()}")

        if '--' in sql_clean or '/*' in sql_clean:
            raise ValueError("SQL comments not allowed")

        subquery_count = sql_lower.count('select') - 1
        if subquery_count > 1:
            raise ValueError("Too many nested subqueries")

        if len(sql_clean) > 1500:
            raise ValueError("Query too long (max 1500 characters)")

        return sql_clean


# ============================================================
# JSON Serializer
# ============================================================
def safe_json_serialize(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, bytes):
        return obj.decode('utf-8', errors='ignore')
    import uuid as uuid_module
    if isinstance(obj, uuid_module.UUID):
        return str(obj)
    raise TypeError(f"Type {type(obj).__name__} not serializable")


# ============================================================
# Workflow
# ============================================================
class AuditTrailAIWorkflow:
    def __init__(self, llm_instance):
        self.llm = llm_instance
        self.graph = self._build_graph()

    def _build_graph(self):
        workflow = StateGraph(ProcessingState)

        def node_generate_sql(state: ProcessingState) -> ProcessingState:
            request_id = state.get("request_id", "")
            try:
                logger.log(f"Generating SQL for: {state['question'][:50]}...", "INFO", request_id)

                clean_question = InputSanitizer.sanitize_question(state["question"])

                prompt = sql_prompt.format(question=clean_question)
                response = invoke_llm_with_retry(self.llm, prompt, request_id)
                sql = response.text.strip()

                sql = re.sub(r'```sql\s*', '', sql)
                sql = re.sub(r'```\s*', '', sql)
                sql = sql.strip()

                if not sql:
                    raise ValueError("Generated SQL is empty")

                if sql.startswith("NO_QUERY:"):
                    friendly_reply = sql[len("NO_QUERY:"):].strip()
                    state["sql_query"] = None
                    state["query_results"] = {"results": [], "row_count": 0, "columns": [], "truncated": False}
                    state["row_count"] = 0
                    state["response_text"] = friendly_reply
                    state["steps_completed"].append("sql_generation")
                    state["steps_completed"].append("no_query_shortcut")
                    logger.log("NO_QUERY detected, skipping SQL execution", "INFO", request_id)
                    return state

                sql = fix_ip_site_column(sql)

                state["sql_query"] = sql
                state["steps_completed"].append("sql_generation")
                logger.log(f"SQL generated: {sql[:120]}...", "INFO", request_id)
                return state

            except Exception as e:
                error_msg = f"SQL Generation: {str(e)}"
                logger.log(f"ERROR: {error_msg}\n{traceback.format_exc()}", "ERROR", request_id)
                state["errors"].append(error_msg)
                return state

        def node_validate_sql(state: ProcessingState) -> ProcessingState:
            if state.get("errors") or state.get("response_text"):
                return state

            request_id = state.get("request_id", "")
            try:
                logger.log("Validating SQL...", "INFO", request_id)
                # 🆕 Retire un ORDER BY invalide (SELECT DISTINCT + colonne absente du SELECT)
                # AVANT la validation stricte, pour ne pas planter sur une requête
                # que l'on peut corriger automatiquement.
                sql = strip_invalid_order_by_for_distinct(state["sql_query"], request_id)
                state["sql_query"] = SQLValidator.validate(sql)
                state["steps_completed"].append("sql_validation")
                logger.log("SQL validated successfully", "INFO", request_id)
                return state

            except Exception as e:
                error_msg = f"Validation: {str(e)}"
                logger.log(f"ERROR: {error_msg}", "ERROR", request_id)
                state["errors"].append(error_msg)
                return state

        def node_execute_query(state: ProcessingState) -> ProcessingState:
            if state.get("errors") or state.get("response_text"):
                return state

            request_id = state.get("request_id", "")
            try:
                logger.log("Executing query on AuditTrail (SQL Server)...", "INFO", request_id)

                with DatabaseManager.get_sql_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(state["sql_query"])
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    rows = cursor.fetchall()
                    data = [dict(zip(columns, row)) for row in rows]

                truncated = len(data) > MAX_RESULT_ROWS
                if truncated:
                    data = data[:MAX_RESULT_ROWS]

                state["query_results"] = {
                    "results": data,
                    "row_count": len(data),
                    "columns": columns,
                    "truncated": truncated
                }
                state["row_count"] = len(data)
                state["steps_completed"].append("database_execution")
                logger.log(f"Query executed: {len(data)} rows", "INFO", request_id)
                return state

            except Exception as e:
                error_msg = f"Database: {str(e)}"
                logger.log(f"ERROR: {error_msg}\n{traceback.format_exc()}", "ERROR", request_id)
                state["errors"].append(error_msg)
                return state

        def node_generate_insights(state: ProcessingState) -> ProcessingState:
            if state.get("errors") or state.get("response_text"):
                return state

            request_id = state.get("request_id", "")
            try:
                logger.log("Generating insights...", "INFO", request_id)

                want_list = is_list_request(state["question"])
                sample_size = LIST_SAMPLE_SIZE if want_list else SUMMARY_SAMPLE_SIZE
                data_sample = state["query_results"]["results"][:sample_size]
                data_sample = truncate_extra_info(data_sample, EXTRA_INFO_MAX_LEN)
                data_str = json.dumps(data_sample, default=safe_json_serialize)

                active_prompt = list_prompt if want_list else insights_prompt

                prompt = active_prompt.format(
                    question=state["question"],
                    data=data_str,
                    row_count=state["row_count"]
                )

                response = invoke_llm_with_retry(self.llm, prompt, request_id)
                insights = response.text.strip()

                if want_list and len(state["query_results"]["results"]) > sample_size:
                    insights += (
                        f"\n\nNote : seules les {sample_size} premières lignes sont listées ici "
                        f"(sur {state['row_count']} au total). Utilisez /ask/export-pdf pour obtenir "
                        f"un export PDF complet avec toutes les lignes."
                    )

                if state["query_results"].get("truncated"):
                    insights += f"\n\nNote: Results limited to {MAX_RESULT_ROWS} rows"

                state["response_text"] = insights
                state["steps_completed"].append("insights_generation")
                logger.log("Insights generated", "INFO", request_id)
                return state

            except Exception as e:
                error_msg = f"Insights: {str(e)}"
                logger.log(f"ERROR: {error_msg}", "ERROR", request_id)
                state["errors"].append(error_msg)
                state["response_text"] = f"Query successfully returned {state['row_count']} rows."
                return state

        def node_save_history(state: ProcessingState) -> ProcessingState:
            request_id = state.get("request_id", "")
            try:
                if state.get("session_id") and state.get("response_text"):
                    DatabaseManager.save_message(state["session_id"], "user", state["question"])
                    DatabaseManager.save_message(state["session_id"], "assistant", state["response_text"])
                state["steps_completed"].append("history_save")
                logger.log("History saved", "INFO", request_id)
            except Exception as e:
                logger.log(f"History save error: {str(e)}", "ERROR", request_id)
            return state

        workflow.add_node("generate_sql", node_generate_sql)
        workflow.add_node("validate_sql", node_validate_sql)
        workflow.add_node("execute_query", node_execute_query)
        workflow.add_node("generate_insights", node_generate_insights)
        workflow.add_node("save_history", node_save_history)

        workflow.set_entry_point("generate_sql")
        workflow.add_edge("generate_sql", "validate_sql")
        workflow.add_edge("validate_sql", "execute_query")
        workflow.add_edge("execute_query", "generate_insights")
        workflow.add_edge("generate_insights", "save_history")
        workflow.add_edge("save_history", END)

        logger.log("Workflow graph built")
        return workflow.compile()

    async def process_question_stream(
        self,
        question: str,
        session_id: Optional[str] = None,
        voice_mode: bool = False,
        user_id: Optional[int] = None
    ) -> AsyncGenerator[str, None]:
        request_id = str(uuid.uuid4())

        if not session_id:
            session_id = DatabaseManager.create_session(voice_mode, user_id=user_id)

        state: ProcessingState = {
            "question": question,
            "session_id": session_id,
            "voice_mode": voice_mode,
            "sql_query": None,
            "query_results": None,
            "row_count": 0,
            "response_text": "",
            "errors": [],
            "steps_completed": [],
            "metadata": {},
            "request_id": request_id
        }

        logger.log(f"Processing request: {question[:50]}...", "INFO", request_id)

        yield f"data: {json.dumps({'type': 'session', 'session_id': session_id})}\n\n"

        step_names = {
            "sql_generation": "Generating SQL query...",
            "sql_validation": "Validating query security...",
            "database_execution": "Querying AuditTrail database...",
            "insights_generation": "Analyzing results...",
            "history_save": "Saving to history..."
        }

        try:
            loop = asyncio.get_running_loop()

            for step_key, step_name in step_names.items():
                yield f"data: {json.dumps({'type': 'step', 'step': step_name})}\n\n"
                await asyncio.sleep(0.1)

            final_state = await loop.run_in_executor(None, self.graph.invoke, state)

            if final_state.get("sql_query"):
                yield f"data: {json.dumps({'type': 'sql', 'query': final_state['sql_query']})}\n\n"

            if final_state.get("errors"):
                error_msg = "I apologize, but I'm unable to process your request at this time. "
                error_msg += "Please try rephrasing your question or contact support if the issue persists."
                yield f"data: {json.dumps({'type': 'error', 'message': error_msg, 'debug': final_state['errors']})}\n\n"
                logger.log(f"Request failed with errors: {final_state['errors']}", "ERROR", request_id)
            else:
                yield f"data: {json.dumps({'type': 'count', 'rows': final_state['row_count']})}\n\n"
                yield f"data: {json.dumps({'type': 'response', 'text': final_state['response_text']})}\n\n"
                logger.log("Request completed successfully", "INFO", request_id)

            yield f"data: {json.dumps({'type': 'complete'})}\n\n"

        except Exception as e:
            logger.log(f"Processing error: {str(e)}\n{traceback.format_exc()}", "ERROR", request_id)
            error_msg = "I apologize, but an unexpected error occurred. Please try again."
            yield f"data: {json.dumps({'type': 'error', 'message': error_msg})}\n\n"


workflow_engine = AuditTrailAIWorkflow(llm)

# ============================================================
# FastAPI
# ============================================================
app = FastAPI(title="Audit Trail AI Assistant", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=500)
    session_id: Optional[str] = None
    voice_mode: bool = False


class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6, max_length=100)


class LoginRequest(BaseModel):
    username: str
    password: str


@app.get("/health")
def health_check():
    sql_status = "unknown"
    try:
        with DatabaseManager.get_sql_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
        sql_status = "connected"
    except Exception as e:
        sql_status = f"error: {str(e)}"

    return {
        "status": "ok",
        "version": "1.0",
        "audittrail_database": sql_status,
        "model": "gemini-flash-lite-latest",
    }


@app.post("/register")
def register(req: RegisterRequest):
    user_id = AuthManager.create_user(req.username, req.password)
    if user_id is None:
        raise HTTPException(status_code=400, detail="Ce nom d'utilisateur existe déjà")
    token = AuthManager.create_token(user_id)
    return {"status": "created", "token": token, "username": req.username}


@app.post("/login")
def login(req: LoginRequest):
    user_id = AuthManager.authenticate(req.username, req.password)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Nom d'utilisateur ou mot de passe incorrect")
    token = AuthManager.create_token(user_id)
    return {"status": "ok", "token": token, "username": req.username}


@app.post("/session")
def create_session(voice_mode: bool = False, user_id: int = Depends(get_current_user)):
    session_id = DatabaseManager.create_session(voice_mode, user_id=user_id)
    return {
        "session_id": session_id,
        "created_at": datetime.now().isoformat(),
        "voice_mode": voice_mode
    }


@app.get("/history/{session_id}")
def get_history(session_id: str):
    history = DatabaseManager.get_history(session_id)
    return {"session_id": session_id, "history": history, "count": len(history)}


@app.delete("/session/{session_id}")
def delete_session(session_id: str):
    DatabaseManager.delete_session(session_id)
    return {"status": "deleted", "session_id": session_id}


@app.post("/ask")
async def ask_question(req: QueryRequest, user_id: int = Depends(get_current_user)):
    """Stream processing steps and results"""
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    if len(req.question) > 500:
        raise HTTPException(status_code=400, detail="Question too long (max 500 characters)")

    return StreamingResponse(
        workflow_engine.process_question_stream(
            question=req.question,
            session_id=req.session_id,
            voice_mode=req.voice_mode,
            user_id=user_id
        ),
        media_type="text/event-stream"
    )


# ============================================================
# Export PDF : toutes les lignes du résultat SQL, sans passer par le LLM
# ============================================================
@app.post("/ask/export-pdf")
def export_pdf(req: QueryRequest, user_id: int = Depends(get_current_user)):
    """Génère un PDF contenant TOUTES les lignes correspondant à la question (jusqu'à
    PDF_MAX_ROWS), en réutilisant le même moteur de génération SQL que /ask mais sans
    passer les données au LLM pour la mise en forme — donc pas de risque de timeout et
    pas de limite artificielle à 50/200 lignes."""
    request_id = str(uuid.uuid4())

    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    try:
        sql = generate_and_validate_sql(req.question, request_id)
    except Exception as e:
        logger.log(f"export_pdf SQL generation/validation error: {str(e)}", "ERROR", request_id)
        raise HTTPException(status_code=400, detail=f"Impossible de générer une requête valide : {str(e)}")

    if sql is None:
        raise HTTPException(
            status_code=400,
            detail="Cette question ne correspond pas à une demande de données exportable en PDF."
        )

    try:
        with DatabaseManager.get_sql_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            data = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.log(f"export_pdf database error: {str(e)}", "ERROR", request_id)
        raise HTTPException(status_code=500, detail=f"Erreur lors de l'exécution de la requête : {str(e)}")

    truncated = len(data) > PDF_MAX_ROWS
    if truncated:
        data = data[:PDF_MAX_ROWS]

    # --- Génération du PDF (même format visuel que la liste affichée dans le chat) ---
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=1.5 * cm, rightMargin=1.5 * cm,
        topMargin=1.5 * cm, bottomMargin=1.5 * cm,
    )
    styles = getSampleStyleSheet()
    row_style = ParagraphStyle(
        "RowStyle",
        parent=styles["Normal"],
        fontSize=9,
        leading=13,
        spaceAfter=8,
    )
    elements = []

    elements.append(Paragraph("Export Audit Trail", styles['Title']))
    elements.append(Paragraph(f"Question : {xml_escape(req.question)}", styles['Normal']))
    elements.append(Paragraph(
        f"{len(data)} résultat(s)"
        + (f" (résultat tronqué à {PDF_MAX_ROWS} lignes)" if truncated else ""),
        styles['Normal']
    ))
    elements.append(Spacer(1, 14))

    if not data:
        elements.append(Paragraph("Aucun résultat trouvé.", styles['Normal']))
    else:
        for row in data:
            elements.append(Paragraph(format_row_for_pdf(row), row_style))

    doc.build(elements)
    buffer.seek(0)

    logger.log(f"PDF export generated: {len(data)} rows", "INFO", request_id)

    return Response(
        content=buffer.read(),
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=audit_export.pdf"}
    )


@app.get("/sessions")
def list_sessions(user_id: int = Depends(get_current_user)):
    sessions = DatabaseManager.list_sessions(user_id)
    return {"sessions": sessions, "count": len(sessions)}


@app.get("/")
def serve_index():
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return {
        "message": "Audit Trail AI Assistant API v1.0",
        "docs": "/docs",
        "health": "/health"
    }


if __name__ == "__main__":
    import uvicorn

    logger.log("=" * 60)
    logger.log("🚀 AUDIT TRAIL AI ASSISTANT v1.0")
    logger.log("=" * 60)
    logger.log("Model: gemini-flash-lite-latest")
    logger.log(f"AuditTrail DB: sqlserver://{SQL_SERVER}/{SQL_DB}")
    logger.log("Server: http://localhost:8000")
    logger.log("=" * 60)

    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")