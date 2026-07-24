# Conversion en projet React

## 1. Installer react-router-dom

Dans le terminal, à la racine de `vite-projet` :

```
npm install react-router-dom
```

## 2. Copier les fichiers

Dans ton dossier `vite-projet/src`, remplace/ajoute :

- `App.jsx` (remplace l'existant)
- `main.jsx` (remplace l'existant)
- `style2.css` (nouveau)
- `pages/Auth.jsx` (nouveau dossier + fichier)
- `pages/Chat.jsx` (nouveau dossier + fichier)
- `assets/logo-snrt.png` (nouveau)

Tu peux supprimer `App.css` et `index.css` si tu n'en as plus besoin — tout le style est maintenant dans `style2.css`.

## 3. Vérifier index.html

Assure-toi qu'il contient bien :

```html
<div id="root"></div>
<script type="module" src="/src/main.jsx"></script>
```

## 4. Lancer

```
npm run dev
```

L'app s'ouvre sur `/auth` (connexion) puis redirige vers `/chat` une fois connecté.

## Ce qui a changé par rapport à ton code original

- **Manipulation du DOM → état React** : au lieu de `document.getElementById(...).textContent = ...`, on utilise `useState` (ex: `messages`, `input`, `error`).
- **Deux fichiers HTML → deux "pages" React** avec `react-router-dom` (`/auth` et `/chat`), au lieu de `window.location.href`.
- **`<img src="logo-snrt.png">`** devient un `import logo from "../assets/logo-snrt.png"` puis `<img src={logo} />` — c'est comme ça que Vite gère les images en React.
- **La logique métier ne change pas** : le fetch vers ton backend FastAPI (`/login`, `/register`, `/ask`), le streaming SSE, le token dans `localStorage` — tout est identique, juste réécrit avec des hooks.
