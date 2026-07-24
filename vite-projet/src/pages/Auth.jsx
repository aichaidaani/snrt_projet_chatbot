import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import logo from "../assets/logo-snrt.png";

const API_URL = "http://127.0.0.1:8000"; // adapte si ton backend tourne ailleurs

function Auth() {
  const navigate = useNavigate();

  const [isLoginMode, setIsLoginMode] = useState(true);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  // Si déjà connecté, redirige directement vers le chat
  useEffect(() => {
    if (localStorage.getItem("auth_token")) {
      navigate("/chat");
    }
  }, [navigate]);

  const toggleMode = () => {
    setIsLoginMode((prev) => !prev);
    setError("");
  };

  const submitAuth = async () => {
    setError("");

    if (!username.trim() || !password) {
      setError("Veuillez remplir tous les champs.");
      return;
    }

    const endpoint = isLoginMode ? "/login" : "/register";
    setLoading(true);

    try {
      const response = await fetch(`${API_URL}${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: username.trim(), password }),
      });

      const data = await response.json();

      if (!response.ok) {
        setError(data.detail || "Une erreur est survenue.");
        return;
      }

      localStorage.setItem("auth_token", data.token);
      localStorage.setItem("username", data.username);
      navigate("/chat");
    } catch (err) {
      setError("Impossible de contacter le serveur.");
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter") submitAuth();
  };

  return (
    <div className="auth-page">
      <div className="auth-container">
        <div className="auth-brand">
          <img src={logo} alt="SNRT" onError={(e) => (e.target.style.display = "none")} />
          <span>SNRT Chatbot</span>
        </div>

        <h2 id="form-title">{isLoginMode ? "Connexion" : "Créer un compte"}</h2>

        <input
          type="text"
          placeholder="Nom d'utilisateur"
          autoComplete="username"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          onKeyDown={handleKeyDown}
        />
        <input
          type="password"
          placeholder="Mot de passe"
          autoComplete="current-password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          onKeyDown={handleKeyDown}
        />

        <button onClick={submitAuth} disabled={loading}>
          {isLoginMode ? "Se connecter" : "Créer le compte"}
        </button>

        <div className="error-msg">{error}</div>

        <div className="toggle-link" onClick={toggleMode}>
          {isLoginMode ? "Pas de compte ? Créer un compte" : "Déjà un compte ? Se connecter"}
        </div>
      </div>
    </div>
  );
}

export default Auth;
