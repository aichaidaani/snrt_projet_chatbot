import { useState, useEffect, useRef } from "react";
import { useNavigate } from "react-router-dom";
import logo from "../assets/logo-snrt.png";

const API_URL = "http://127.0.0.1:8000"; // adapte si ton backend tourne ailleurs

function Chat() {
  const navigate = useNavigate();
  const authToken = localStorage.getItem("auth_token");

  // 🆕 On ne pré-remplit plus les messages avec la phrase d'accueil.
  // Tant que "messages" est vide, on affiche l'écran d'accueil centré (logo + titre).
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);
  const [typingText, setTypingText] = useState(null); // texte du message "AI is typing..."
  const [sessions, setSessions] = useState([]); // liste des conversations pour la sidebar
  const sessionIdRef = useRef(localStorage.getItem("audit_session_id") || null);
  const chatBoxRef = useRef(null);

  // 🆕 État pour l'aperçu / export PDF
  const [pdfModalOpen, setPdfModalOpen] = useState(false);
  const [pdfLoading, setPdfLoading] = useState(false);
  const [pdfError, setPdfError] = useState("");
  const [pdfUrl, setPdfUrl] = useState(null);
  const [pdfQuestion, setPdfQuestion] = useState("");

  const storedUsername = localStorage.getItem("username") || "Utilisateur";

  // Vérifie l'authentification avant toute chose
  useEffect(() => {
    if (!authToken) {
      navigate("/auth");
    }
  }, [authToken, navigate]);

  // Auto-scroll vers le bas à chaque nouveau message
  useEffect(() => {
    if (chatBoxRef.current) {
      chatBoxRef.current.scrollTop = chatBoxRef.current.scrollHeight;
    }
  }, [messages, typingText]);

  // 🆕 Nettoyage de l'URL blob quand la modale se ferme ou que le composant se démonte,
  // pour éviter une fuite mémoire (les object URLs ne sont pas libérées automatiquement)
  useEffect(() => {
    return () => {
      if (pdfUrl) URL.revokeObjectURL(pdfUrl);
    };
  }, [pdfUrl]);

  // Charge la liste des conversations de l'utilisateur
  const loadSessions = async () => {
    try {
      const response = await fetch(`${API_URL}/sessions`, {
        headers: { Authorization: `Bearer ${authToken}` },
      });
      if (!response.ok) return;
      const data = await response.json();
      setSessions(data.sessions || []);
    } catch (err) {
      console.error("Impossible de charger l'historique :", err);
    }
  };

  useEffect(() => {
    if (authToken) loadSessions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authToken]);

  const addMessage = (text, className) => {
    setMessages((prev) => [...prev, { text, className }]);
  };

  const logout = () => {
    localStorage.removeItem("auth_token");
    localStorage.removeItem("username");
    localStorage.removeItem("audit_session_id");
    navigate("/auth");
  };

  const newChat = () => {
    sessionIdRef.current = null;
    localStorage.removeItem("audit_session_id");
    setMessages([]); // 🆕 retour à l'écran d'accueil vide
  };

  // Ouvre une conversation existante depuis la sidebar
  const openSession = async (sessionId) => {
    try {
      const response = await fetch(`${API_URL}/history/${sessionId}`);
      if (!response.ok) return;
      const data = await response.json();

      const loadedMessages = (data.history || []).map((msg) => ({
        text: msg.content,
        className: msg.role === "user" ? "user-message" : "bot-message",
      }));

      sessionIdRef.current = sessionId;
      localStorage.setItem("audit_session_id", sessionId);
      setMessages(loadedMessages); // 🆕 si vide, l'écran d'accueil s'affichera
    } catch (err) {
      console.error("Impossible de charger la conversation :", err);
    }
  };

  // 🆕 Supprime une conversation depuis la sidebar
  const deleteSession = async (e, sessionId) => {
    e.stopPropagation(); // évite de déclencher openSession()

    try {
      const response = await fetch(`${API_URL}/session/${sessionId}`, {
        method: "DELETE",
        headers: { Authorization: `Bearer ${authToken}` },
      });

      if (!response.ok) {
        console.error("Échec de la suppression de la session");
        return;
      }

      // Retire la session de la liste locale
      setSessions((prev) => prev.filter((s) => s.session_id !== sessionId));

      // Si la conversation supprimée est celle actuellement ouverte, on repart sur un nouveau chat
      if (sessionIdRef.current === sessionId) {
        newChat();
      }
    } catch (err) {
      console.error("Impossible de supprimer la conversation :", err);
    }
  };

  const sendMessage = async () => {
    const message = input.trim();
    if (message === "") return;

    addMessage(message, "user-message");
    setInput("");
    setSending(true);
    setTypingText("AI is typing...");

    try {
      const response = await fetch(`${API_URL}/ask`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${authToken}`,
        },
        body: JSON.stringify({
          question: message,
          session_id: sessionIdRef.current,
          voice_mode: false,
        }),
      });

      if (response.status === 401) {
        setTypingText(null);
        logout();
        return;
      }

      if (!response.ok || !response.body) {
        throw new Error(`HTTP ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8");
      let buffer = "";

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        // Le stream SSE envoie des blocs séparés par "\n\n"
        const parts = buffer.split("\n\n");
        buffer = parts.pop(); // garde le reste incomplet pour le prochain chunk

        for (const part of parts) {
          if (!part.startsWith("data: ")) continue;
          const jsonStr = part.slice(6).trim();
          if (!jsonStr) continue;

          let payload;
          try {
            payload = JSON.parse(jsonStr);
          } catch {
            continue;
          }

          switch (payload.type) {
            case "session":
              sessionIdRef.current = payload.session_id;
              localStorage.setItem("audit_session_id", payload.session_id);
              break;

            case "step":
              setTypingText(payload.step); // ex: "Generating SQL query..."
              break;

            case "sql":
              // pas affiché dans l'UI actuelle, mais dispo si besoin
              break;

            case "error":
              setTypingText(null);
              addMessage(payload.message, "bot-message");
              break;

            case "response":
              setTypingText(null);
              addMessage(payload.text, "bot-message");
              break;

            case "complete":
              // la conversation vient d'être enregistrée côté serveur : on rafraîchit la sidebar
              loadSessions();
              break;

            default:
              break;
          }
        }
      }
    } catch (err) {
      setTypingText(null);
      addMessage("Erreur de connexion au serveur. Vérifie que l'API tourne bien.", "bot-message");
      console.error(err);
    } finally {
      setSending(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter") sendMessage();
  };

  // 🆕 Génère le PDF pour une question donnée et ouvre la modale d'aperçu
  const exportPdf = async (question) => {
    setPdfQuestion(question);
    setPdfModalOpen(true);
    setPdfLoading(true);
    setPdfError("");
    setPdfUrl(null);

    try {
      const response = await fetch(`${API_URL}/ask/export-pdf`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${authToken}`,
        },
        body: JSON.stringify({ question }),
      });

      if (response.status === 401) {
        setPdfModalOpen(false);
        logout();
        return;
      }

      if (!response.ok) {
        const errData = await response.json().catch(() => ({}));
        throw new Error(errData.detail || `Erreur HTTP ${response.status}`);
      }

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      setPdfUrl(url);
    } catch (err) {
      setPdfError(err.message || "Impossible de générer le PDF.");
      console.error(err);
    } finally {
      setPdfLoading(false);
    }
  };

  const closePdfModal = () => {
    setPdfModalOpen(false);
    if (pdfUrl) {
      URL.revokeObjectURL(pdfUrl);
      setPdfUrl(null);
    }
    setPdfError("");
  };

  if (!authToken) return null; // évite un flash de contenu avant la redirection

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <img src={logo} alt="SNRT" onError={(e) => (e.target.style.display = "none")} />
          <span>SNRT Chatbot</span>
        </div>

        <div className="sidebar-item" onClick={newChat}>
          ＋ Nouveau chat
        </div>

        {/* Historique réel des conversations, chargé depuis /sessions */}
        <div className="sidebar-history">
          {sessions.length === 0 && (
            <div className="sidebar-history-item">Aucune conversation</div>
          )}
          {sessions.map((s) => (
            <div
              key={s.session_id}
              className="sidebar-history-item"
              onClick={() => openSession(s.session_id)}
              style={{
                fontWeight: s.session_id === sessionIdRef.current ? 600 : 400,
              }}
              title={s.title}
            >
              <span className="sidebar-history-title">{s.title}</span>
              <button
                className="sidebar-delete-btn"
                title="Supprimer la conversation"
                onClick={(e) => deleteSession(e, s.session_id)}
              >
                🗑
              </button>
            </div>
          ))}
        </div>

        <div className="sidebar-footer">
          <div className="sidebar-user">
            <span className="avatar-circle">{storedUsername.charAt(0).toUpperCase()}</span>
            <span>{storedUsername}</span>
          </div>
          <button className="logout-icon-btn" title="Déconnexion" onClick={logout}>
            Déconnexion
          </button>
        </div>
      </aside>

      <main className="main">
        <div className="main-header">Audit Trail — Assistant</div>

        <div className="chat-box" ref={chatBoxRef}>
          {/* 🆕 Écran d'accueil centré tant qu'aucun message n'a été échangé */}
          {messages.length === 0 && !typingText ? (
            <div className="empty-state">
              <img src={logo} alt="SNRT" className="empty-state-logo" onError={(e) => (e.target.style.display = "none")} />
              <div className="empty-state-title">Assistant</div>
              <div className="empty-state-subtitle">
                Posez-moi une question sur l'Audit Trail (connexions, suppressions, activité utilisateur...)
              </div>
            </div>
          ) : (
            messages.map((msg, i) => (
              <div key={i} className={`message ${msg.className}`}>
                <div>{msg.text}</div>
                {/* 🆕 Bouton d'export PDF, uniquement sur les messages de l'utilisateur
                    (chaque question posée peut être ré-exportée en PDF complet) */}
                {msg.className === "user-message" && (
                  <button
                    className="export-pdf-btn"
                    title="Exporter cette question en PDF"
                    onClick={() => exportPdf(msg.text)}
                  >
                    📄 PDF
                  </button>
                )}
              </div>
            ))
          )}
          {typingText && <div className="message bot-message">{typingText}</div>}
        </div>

        <div className="input-area">
          <div className="input-pill">
            <input
              id="user-input"
              type="text"
              placeholder="Question..."
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
            />
            <button id="send-button" onClick={sendMessage} disabled={sending}>
              ➤
            </button>
          </div>
        </div>
      </main>

      {/* 🆕 Panneau PDF plein écran, ancré à droite */}
      {pdfModalOpen && (
        <div className="pdf-panel">
          <div className="pdf-panel-header">
            <span title={pdfQuestion}>Export PDF — {pdfQuestion}</span>
            <div className="pdf-panel-actions">
              {pdfUrl && (
                <a
                  href={pdfUrl}
                  download="audit_export.pdf"
                  className="pdf-download-btn"
                >
                  ⬇ Télécharger
                </a>
              )}
              <button className="pdf-close-btn" onClick={closePdfModal}>
                ✕
              </button>
            </div>
          </div>

          <div className="pdf-panel-body">
            {pdfLoading && <div className="pdf-status">⏳ Génération du PDF en cours...</div>}
            {pdfError && <div className="pdf-status pdf-status-error">❌ {pdfError}</div>}
            {pdfUrl && !pdfLoading && (
              <iframe title="Aperçu PDF" src={pdfUrl} className="pdf-iframe" />
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export default Chat;