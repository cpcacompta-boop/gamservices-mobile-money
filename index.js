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
  // Verrouillage anti-force brute, stocké en base (survit aux redémarrages)
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_attempts INT DEFAULT 0`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ`);
  // Infos de la personne rattachée au compte (équipe)
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS nom TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS contact TEXT`);
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
function requireRole(...roles) {
  return (req, res, next) => roles.includes(req.user.role)
    ? next() : res.status(403).json({ error: 'Acces reserve au superviseur' });
}

/* =====================================================================
   ROUTES
   ===================================================================== */
app.get('/health', wrap(async (req, res) => {
  await pool.query('SELECT 1');
  res.json({ status: 'ok', database: 'connectee' });
}));

// Connexion (public) — verrouillage anti-force brute PERSISTANT (en base)
const MAX_ATTEMPTS = 3;   // nombre de tentatives avant blocage
const LOCK_MINUTES = 5;   // durée du blocage

app.post('/login', wrap(async (req, res) => {
  const { username, password } = req.body;
  const r = await pool.query('SELECT * FROM users WHERE username=$1 AND actif=TRUE', [username]);
  const u = r.rows[0];

  // Compte inconnu : message générique (pas de blocage possible sans compte réel)
  if (!u) return res.status(401).json({ error: 'Identifiant ou mot de passe incorrect' });

  // Déjà verrouillé ? (la vérif est en base → vaut pour tous les navigateurs et survit aux redémarrages)
  if (u.locked_until && new Date(u.locked_until).getTime() > Date.now()) {
    const remaining = Math.ceil((new Date(u.locked_until).getTime() - Date.now()) / 1000);
    return res.status(423).json({ error: 'Compte temporairement verrouillé', locked: true, remaining });
  }

  const ok = verifyPass(password, u.pass_hash);
  if (!ok) {
    const newCount = (u.failed_attempts || 0) + 1;
    if (newCount >= MAX_ATTEMPTS) {
      await pool.query(
        "UPDATE users SET failed_attempts=0, locked_until = now() + interval '5 minutes' WHERE id=$1",
        [u.id]);
      return res.status(423).json({ error: 'Trop de tentatives', locked: true, remaining: LOCK_MINUTES * 60 });
    }
    await pool.query('UPDATE users SET failed_attempts=$1 WHERE id=$2', [newCount, u.id]);
    return res.status(401).json({ error: 'Identifiant ou mot de passe incorrect', attemptsLeft: MAX_ATTEMPTS - newCount });
  }

  // Succès : on efface le compteur et le verrou
  await pool.query('UPDATE users SET failed_attempts=0, locked_until=NULL WHERE id=$1', [u.id]);
  const token = crypto.randomBytes(24).toString('hex');
  await pool.query('INSERT INTO sessions (token, user_id) VALUES ($1,$2)', [token, u.id]);
  res.json({ token, user: { id: u.id, username: u.username, role: u.role, linked_id: u.linked_id } });
}));

// A partir d'ici, connexion requise
app.use(requireAuth);

app.get('/me', wrap(async (req, res) => {
  const u = req.user;
  res.json({ id: u.id, username: u.username, role: u.role, nom: u.nom, linked_id: u.linked_id });
}));

app.post('/logout', wrap(async (req, res) => {
  const token = getToken(req);
  if (token) await pool.query('DELETE FROM sessions WHERE token=$1', [token]);
  res.json({ ok: true });
}));

/* ---- COMPTE ÉQUIPE : gestion des comptes (SUPERVISEUR uniquement) ---- */
app.get('/users', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const r = await pool.query(
    `SELECT id, username, role, nom, contact, actif, created_at
     FROM users ORDER BY role, username`);
  res.json(r.rows);
}));

app.post('/users', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const { username, password, role, nom, contact } = req.body;
  if (!username || !password || !role)
    return res.status(400).json({ error: 'Identifiant, mot de passe et rôle sont obligatoires' });
  if (!['SUPERVISEUR', 'MASTER', 'COMMERCIAL', 'PDV'].includes(role))
    return res.status(400).json({ error: 'Rôle invalide' });
  const exists = await pool.query('SELECT 1 FROM users WHERE username=$1', [username]);
  if (exists.rows.length) return res.status(400).json({ error: 'Cet identifiant existe déjà' });
  const r = await pool.query(
    `INSERT INTO users (username, pass_hash, role, nom, contact)
     VALUES ($1,$2,$3,$4,$5) RETURNING id, username, role, nom, contact, actif`,
    [username, hashPass(password), role, nom || null, contact || null]);
  res.json(r.rows[0]);
}));

app.post('/users/:id/password', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  if (!req.body.password) return res.status(400).json({ error: 'Nouveau mot de passe requis' });
  await pool.query('UPDATE users SET pass_hash=$1, failed_attempts=0, locked_until=NULL WHERE id=$2',
    [hashPass(req.body.password), req.params.id]);
  await pool.query('DELETE FROM sessions WHERE user_id=$1', [req.params.id]);
  res.json({ ok: true });
}));

app.delete('/users/:id', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const t = await pool.query('SELECT role FROM users WHERE id=$1', [req.params.id]);
  if (t.rows[0] && t.rows[0].role === 'SUPERVISEUR') {
    const c = await pool.query("SELECT COUNT(*) c FROM users WHERE role='SUPERVISEUR' AND actif=TRUE");
    if (Number(c.rows[0].c) <= 1) return res.status(400).json({ error: 'Impossible de supprimer le dernier superviseur' });
  }
  await pool.query('DELETE FROM users WHERE id=$1', [req.params.id]);
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
