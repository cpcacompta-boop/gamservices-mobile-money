/* =====================================================================
   GAMServices Holding — Backend minimal (authentification uniquement)
   On repart de zéro : seule la connexion est gérée pour l'instant.
   ===================================================================== */
const express = require('express');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '15mb' }));

// Sert le frontend : index.html qu'il soit à côté de index.js ou dans "public"
const STATIC_DIR = [__dirname, path.join(__dirname, 'public')]
  .find(d => fs.existsSync(path.join(d, 'index.html'))) || __dirname;
app.use(express.static(STATIC_DIR));

// Base de données (Neon par défaut avec SSL ; DB_SSL=off pour une base locale)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DB_SSL === 'off' ? false : { rejectUnauthorized: false }
});

/* =====================================================================
   MIGRATION AUTOMATIQUE (aucun SQL à lancer à la main)
   ===================================================================== */
async function migrate() {
  await pool.query(`CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    pass_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    linked_id INTEGER,
    actif BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  await seedSuperviseur();
  console.log('\u2714 Migration terminee : base a jour.');
}

// Compte Superviseur par defaut : Superviseur / Admin@123
async function seedSuperviseur() {
  const r = await pool.query("SELECT id FROM users WHERE username=$1", ['Superviseur']);
  if (r.rows.length === 0) {
    await pool.query(
      "INSERT INTO users (username, pass_hash, role) VALUES ($1,$2,'SUPERVISEUR')",
      ['Superviseur', hashPass('Admin@123')]);
    console.log('\u2714 Compte Superviseur cree (Superviseur / Admin@123)');
  }
}

/* =====================================================================
   HELPERS
   ===================================================================== */
const wrap = (fn) => (req, res) => fn(req, res).catch(err => {
  console.error(err);
  res.status(400).json({ error: err.message });
});

function hashPass(pw) {
  const salt = crypto.randomBytes(16).toString('hex');
  const h = crypto.scryptSync(String(pw), salt, 64).toString('hex');
  return salt + ':' + h;
}
function verifyPass(pw, stored) {
  try {
    const [salt, h] = String(stored).split(':');
    const hh = crypto.scryptSync(String(pw), salt, 64).toString('hex');
    return crypto.timingSafeEqual(Buffer.from(h, 'hex'), Buffer.from(hh, 'hex'));
  } catch (e) { return false; }
}
function getToken(req) {
  const h = req.headers['authorization'] || '';
  return h.startsWith('Bearer ') ? h.slice(7) : (req.headers['x-auth-token'] || '');
}
async function requireAuth(req, res, next) {
  try {
    const token = getToken(req);
    if (!token) return res.status(401).json({ error: 'Non authentifie' });
    const r = await pool.query(
      `SELECT u.* FROM sessions s JOIN users u ON u.id = s.user_id
       WHERE s.token = $1 AND u.actif = TRUE`, [token]);
    if (r.rows.length === 0) return res.status(401).json({ error: 'Session expiree' });
    req.user = r.rows[0];
    next();
  } catch (e) { res.status(500).json({ error: e.message }); }
}

/* =====================================================================
   ROUTES
   ===================================================================== */
app.get('/health', wrap(async (req, res) => {
  await pool.query('SELECT 1');
  res.json({ status: 'ok', database: 'connectee' });
}));

// Connexion (public)
app.post('/login', wrap(async (req, res) => {
  const { username, password } = req.body;
  const r = await pool.query('SELECT * FROM users WHERE username=$1 AND actif=TRUE', [username]);
  if (r.rows.length === 0 || !verifyPass(password, r.rows[0].pass_hash))
    return res.status(401).json({ error: 'Identifiant ou mot de passe incorrect' });
  const u = r.rows[0];
  const token = crypto.randomBytes(24).toString('hex');
  await pool.query('INSERT INTO sessions (token, user_id) VALUES ($1,$2)', [token, u.id]);
  res.json({ token, user: { id: u.id, username: u.username, role: u.role, linked_id: u.linked_id } });
}));

// A partir d'ici, connexion requise
app.use(requireAuth);

app.get('/me', wrap(async (req, res) => {
  const u = req.user;
  res.json({ id: u.id, username: u.username, role: u.role, linked_id: u.linked_id });
}));

app.post('/logout', wrap(async (req, res) => {
  const token = getToken(req);
  if (token) await pool.query('DELETE FROM sessions WHERE token=$1', [token]);
  res.json({ ok: true });
}));

// Filet de securite : toute page non-API renvoie index.html
app.get('*', (req, res) => {
  const file = path.join(STATIC_DIR, 'index.html');
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(200).send('Backend GAMServices en ligne.');
});

/* =====================================================================
   DEMARRAGE
   ===================================================================== */
const PORT = process.env.PORT || 3000;
migrate()
  .then(() => app.listen(PORT, () => console.log('\u2714 GAMServices demarre sur le port ' + PORT)))
  .catch(err => {
    console.error('\u2718 Echec migration au demarrage :', err.message);
    app.listen(PORT, () => console.log('\u26a0 Serveur demarre (migration en erreur) port ' + PORT));
  });
