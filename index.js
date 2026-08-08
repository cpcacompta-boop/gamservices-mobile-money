/* =====================================================================
   GAMServices Holding — Backend minimal (authentification uniquement)
   On repart de zéro : seule la connexion est gérée pour l'instant.
   ===================================================================== */
const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { Pool } = require('pg');
const cors = require('cors');
const { WebSocketServer } = require('ws');
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
   TEMPS RÉEL (WebSocket) — positions live, blocage instantané, modifs
   ===================================================================== */
const wsClients = new Set(); // chaque ws porte .userId et .role
function wsSend(ws, obj){ try{ if(ws.readyState === 1) ws.send(JSON.stringify(obj)); }catch(e){} }
function sendToUser(userId, obj){ wsClients.forEach(ws=>{ if(ws.userId === Number(userId)) wsSend(ws, obj); }); }
function broadcastToSupervisors(obj){ wsClients.forEach(ws=>{ if(ws.role === 'SUPERVISEUR') wsSend(ws, obj); }); }
function kickUser(userId){ wsClients.forEach(ws=>{ if(ws.userId === Number(userId)){ wsSend(ws, { type:'blocked' }); try{ ws.close(); }catch(e){} } }); }

// Vérifie les recharges UV encaissées en attente depuis un jour précédent (paiement "oublié")
// -> alerte le Master + le superviseur, la recharge RESTE en attente (aucune fermeture automatique)
async function checkOverdueRecharges() {
  try {
    const r = await pool.query(
      `SELECT rc.*, m.username AS master_username FROM uv_recharges rc
       JOIN users m ON m.id = rc.master_id
       WHERE rc.statut='EN_ATTENTE' AND rc.alert_sent=FALSE AND rc.created_at < date_trunc('day', now())`);
    for (const rc of r.rows) {
      sendToUser(rc.master_id, { type: 'recharge_alert', recharge: rc });
      broadcastToSupervisors({ type: 'recharge_alert', recharge: rc });
      await pool.query('UPDATE uv_recharges SET alert_sent=TRUE WHERE id=$1', [rc.id]);
    }
  } catch (e) { console.error('Verification recharges en retard :', e.message); }
}

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
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS prenoms TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS code TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS photo TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS piece_recto TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS piece_verso TEXT`);
  // Oblige à définir un nouveau mot de passe (1er login ou après réinitialisation par le superviseur)
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN DEFAULT FALSE`);
  // Champs Point de Vente (PDV)
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS nom_commercial TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS ville TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS quartier TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS zone TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS situation_geo TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS gps TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS nom_responsable TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS contact_responsable TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS nom_gerant TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS contact_gerant TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS photo_local TEXT`);
  // Géolocalisation temps réel des agents
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_lat DOUBLE PRECISION`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_lng DOUBLE PRECISION`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_accuracy DOUBLE PRECISION`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS loc_updated_at TIMESTAMPTZ`);
  await pool.query(`CREATE TABLE IF NOT EXISTS positions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    lat DOUBLE PRECISION, lng DOUBLE PRECISION, accuracy DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  await pool.query(`CREATE INDEX IF NOT EXISTS positions_user_time ON positions(user_id, created_at DESC)`);
  // Zones : un commercial est responsable d'une zone (avec ses quartiers) ; un suppléant peut le remplacer
  await pool.query(`CREATE TABLE IF NOT EXISTS zones (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  // Ces colonnes ont été ajoutées après la 1ère version de la table : on les
  // rajoute ici avec ALTER TABLE (comme pour "users") pour que ça marche
  // aussi sur une base déjà existante, sans jamais perdre de données.
  await pool.query(`ALTER TABLE zones ADD COLUMN IF NOT EXISTS responsable_id INTEGER REFERENCES users(id) ON DELETE SET NULL`);
  await pool.query(`ALTER TABLE zones ADD COLUMN IF NOT EXISTS suppleant_id INTEGER REFERENCES users(id) ON DELETE SET NULL`);
  await pool.query(`ALTER TABLE zones ADD COLUMN IF NOT EXISTS quartiers TEXT`);
  await pool.query(`CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  /* ---- Fonds UV / FCFA du Master (crédité manuellement par le superviseur) ---- */
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS solde_uv NUMERIC NOT NULL DEFAULT 0`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS solde_fcfa NUMERIC NOT NULL DEFAULT 0`);
  // Historique des crédits accordés par le superviseur à un Master
  await pool.query(`CREATE TABLE IF NOT EXISTS fund_credits (
    id SERIAL PRIMARY KEY,
    master_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    uv NUMERIC NOT NULL DEFAULT 0,
    fcfa NUMERIC NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  // Chaque recharge/vente d'UV qu'un Master fait à un PDV. "EN_ATTENTE" tant que le PDV n'a pas payé.
  await pool.query(`CREATE TABLE IF NOT EXISTS uv_recharges (
    id SERIAL PRIMARY KEY,
    master_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    pdv_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    montant_uv NUMERIC NOT NULL,
    montant_fcfa NUMERIC NOT NULL DEFAULT 0,
    statut TEXT NOT NULL DEFAULT 'EN_ATTENTE',
    alert_sent BOOLEAN DEFAULT FALSE,
    paid_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  await pool.query(`CREATE INDEX IF NOT EXISTS uv_recharges_master ON uv_recharges(master_id, created_at DESC)`);
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
  res.json({ token, user: { id: u.id, username: u.username, role: u.role, linked_id: u.linked_id, mustChangePassword: !!u.must_change_password } });
}));

/* ---- PWA : manifest, icônes, service worker (public, mode vraie application) ---- */
const ICON_192 = "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";
const ICON_512 = "iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAMAAADDpiTIAAAAwFBMVEXl3uUiXuyXmqT1JS3zXF9lkvC1ye30qa2UsPBrboV8gZA8RWGnFmznuMYlL1BIdeuRJpDZIVJQVmwWOt1hJaOlXKTXbZaWc8Y7Lbt3E3pBO8PMP4b2RDzwgn8WIkr7+/sFS+r6Bwz6ChHn5+jX2NkDSfIqNFNES2TFxskiLEx0eIhlaXsJUeszO1htcYSGiZa1trv6FBaXmaRcYnYJUvOpq7NLUmq7vMI6Ql1VW3L1qKkUHkPH2PajpawAPOhTWG0FTBGBAAA50UlEQVR42u2dCXviuLKwhRMIkOlkOt0z59xviQEbrxizmaUD4f//q6uSbSx5SVgsg5Oq5zlnOuk0AetV7SqRV5RvLQQfAQKAggCgIAAoCAAKAoCCAKAgACgIAAoCgIIAoCAAKAgACgKAggCgIAAoCAAKAoCCAKAgACgIAAoCgIIAoCAAKAgACgKAggCgIAAoCAAKAoCCAKAgACgIAAoCgIIAoCAAKAgACgKAggCgIAAoCAAKAoCCAKAgACgIAAoCgIIAoCAAKAgACgKAggCgIAAoCAAKAoCCAKAgACgIAAoCgIIAoCAAKAgACgKAggCgIAAoCAAKAoCCAKAgACgIAAoCgIIAoCAAKAgACgKAggCgIAAoCAAKAoCCAKAgACgIAAoCgIIAoCAAKAgAyjcE4O3VMIz1cj93Y5nPN/u9ufYM44n+LQLwVcUzly8zJWjpmqbmiqZpuhUEM981H58QgC8k67mzW+kFy14AgzVROksbAai5GKajWKRwwxMQDaTgR/Sd73oIQD3lcay0tPSCklYw644786W5tuP9bdu2sTaXm3ln7M+UFknrCj1Qlo8IQJ3E3jgTXVjDVeC7y0fjWG/hxZkFlsAB2c09BKAezt44IPymD5S5d5ZPZ6xdJdB5v8DfPCEANy1Pm5nF7XvF2RgX8zT3VwkF2sR5RABudutPDlpbWymbElfKdIMELKK8IAC3F+o5K+2gqpXLN34OX+7soAn02RIBuKW937WSpXENeb/I7B5cQ91/RABuY/WdVbz6lnLMxqRO/mbjOr4/Y6Ioiu/MN5uX46IEr6PEDFhjAwG4tswDLXbPOusPM/nGsuMoE0I+SAlquk4mivNZCtBIfmmwQQCuufn92CqvPtqMkA+cnJQK1khr5u8/eEm7EzucpGsgANeRTbwPSbE59pbdVEYn3uqW1VqtVhMq9D8tK7dEpCuzufmp46HNTASgcjEcK/b6Ch6/vRkHRBNW3WLm3jQfjayON7zHR3Pu+LPAEv6Vqrf8ef4mfzPjuGAyRwCq1f2xI6bMC+y9P9F4jR7446XxdGw6yViOuzuLTwKSouCiE3mguoMAVCbmTvsoEnt0uMXXVsr45enM3+POVtqnSUAzglF3nhCAKmSpxEGYnaf3Z4Qr43XNSz006kDuElVAZptcbyD8Cc03EADpyx+p3CAn4jfGgZbo7E551Ttjrhw8SU3p5Cyza9UVgXoBYEa7f5dVxoZ7SNDou075pVvDPWgCLY++eYRAvi/g610E4GJZ76Kwa51VDLNDek6RF5QtFeuDRHCkBYibzUftVaKuPATgMs9/Fj76bOLFVPSDnyb7KT9248RzK5t72oR/Z6UdBZuQuweVOAjA+WL7bItr3bTnZ4/jfMzErcYAe3EKQsuWHvat0EMROVTU+37/nqgmAnCuuHpYiU8v8Tre/JZTpYZdxzno1Tz/nWo+9y1Hfej3B/0HBOBsvTsJ91Xa9m+Uq5XnbXdSFI06TFeRAxovKpkOBv17NUATcJ7/HRr/SXr/jKOIP7hSHjZWA3o69ovesBLpJIsagEF/SrQnBOAs7c82lJ5aZduJky/rK763+SqKS1IIPK6SkBAcgOmg/0fdYxRwjscVsEfs27nLbznXPryzDCJ/0MizA6s1NQAP/cGUGgAF8wBnyDh8jqL2N/xQ+Ss30Y3xGKYgNMXLsQO6bZE7agB+qhYmgs7Y/szR0l1x93e1onTQtd5mmIPUUmpqSaNF12cOwICoLwjAmdZ/95QTZqm3s/xsu4cIpGvCM2UJBgAiwJutFt8uAPYsx/nbhDmY3frW3q0XGoJ0ElCnBgAcgMkrAnCqd8V2+k7wrcww9lbWt/iGvZxoFQzAtH9H9FcE4ETpMqsqWH/bz88H3IxEfM4OrsBe/dOfTiEFuDQQgJNs6iSTUX/r6LmFlpuSuR6arbAeaBMyZRFg11b1NQJwvOxZmCf4TSarsujjG09bvIYhyoQtdxAaAHX1Sj8PGSMAx4rD1lrQ9KH2V2ows8XYhUlAm34MlgIkxLBZYnCHABwnzJkK+LU2me9v1eRAZhipKJ5KoAb4/1XqyLyxz9QyEIAjnGm2W7qZ7a/VqO/a11Tthah3/X7/p7p7O+Q0dBMB+NSTBvOv8Z7ekmTjwVsXz3LDCHBKSKTKTOYfzhGAT9QnbBTLyzgEtTt5s1cf6PrzNcAwsnEQgE/dP97VC5/arnbN1oYWpQCVtHMzQwA+cf948z/XMumgekgcAVoe3wXQYXwjAEXCAih+tVnH1+qT7Q9D/pgs6f+OVhUvG/rj0j6JCwYAIkAvEJT+EoBePSEAuQsJ2l7bp9V/8Ybx9s5MWYkDHzSdrBRlvDeO+F3U2ZBkWjxCpn3oAnQdlQjx/6Oe9nEQgHi5IXrmE6bMbSb53p8395XDwrOBr9vtlv03GRTlmPYHO5S0Wi2i+lI+ytuKNQHcqZM3Fv1Za54N+JiPCEBm/eHBECPlD+apf3sTjf8l21a72XxugPRA2J+em812i0SHQzsFGXhfbb733puSPDKHNQEMqAF4fV2nw1qWFdRMBCC1/npKI/v5HvPTPDyht23Tladr/v5rCMLWfxgLoPDcDCmw/Lz8YYsCMOpJAsBUyZTVAN3E2vCOACPgEQEQVDqs6srmvWgq4/SPLVnjDWk3G8Phr3DVF6PRqBcJ/eOCCZDxTikIIdAzvYM0SGv0RlQDSOnU19UwApzxrqySdnZMBCC1/kHaH0g9orUCq7ltNkZ0cdlq9z4TCkGzzRjwTdEF2FJwenIAmMVNIAeemSMQpMLdWyHgFgBgjpGSnKlldjPloW8mbO83hAXOMHAwB9Hfw9ejkAGrm/jeRG0OF4thk3pppX+YedwFyMUz67SBuyECbgAAI7X/zfSGiQ7f080Pe59b9GGo7un3FmAKQhPA/z0sM0Ui1ANaMPbewsdPGgsGQPm92rbG2sDvRfciVGleioBHBODg/3Ox8ibj/m2gGaTVTG3vcH1DAthfvb+/D8WfiX1C+lehP6Ct/E2HLkYTkBk2VFJ6g8EhBZj6/iS14jdDwPUBgM0RvHH2GQ4C8U71JNT9dLmHycKG+z7x+ZvtNo0JaVT43BiBB0i1w1DAAfyBKDwkzVCNNFSt7EwQSwFOp0T1sq6BqPXZ1x4C8Ab+/upVXH8uaHqCB9VuCKofdj4z/0Pq6LdbW/FSIEJaNExo9IbDtMr4Bbag1Wo2otdqsEi95BRg6ACMs38HwYC2FHWCZX97ABTxMXRSRXM4BtKC5RcEDHgv2dF0zXU293PVsuJcIGlBrDhMe4jvTBewb1PXgKglK+EfLAV4n9/9xVIbKQJa3x0A2N+cIXbEXbKmz2j7/Itf/2Hk3MerrweKa3rG4cZHwzNdJRoX1Qq9xnyB+KDsmR1KGAEQYhRkCKnsxYzQ5HsDwBbcEL9M1sShS9zu/RJDP6q+h4tm2COsjAtU+JMZDnMm7cZ7MQDDbbkArFUyYAagqHlxDO86yUrZJBPufDMANuKCjwUdaVPvgDyn1g/SP8Pm9pipIEZnRZj/UEhAr1VuixaJUoD+Bz6ioANYAOx/XwBMeBxmwdN5hO3fGC7E9aeuPdv9K/cY9ymc4dFKOwMHaZXaoRWnAMmHUYLgB7AOyM53BcDTBIdvLtj/uUajNS7yi7dtA5Y/OFpz251QC6RAiiLDttotU51RB6Df/8Sv6IjQs6THyzcFwBL0nynsDYdt3EVm4zbpeiqnue7gSdDIPx+A8vqzjDgF6B/h9jwKZk8zviUAYgKAqYMNFxy2abSXWv932P7kZLPNBorkuQKlNgQoUQrw08DOEbPCEBxa3xEAOP5LnpIMCmcOnmh81Ex776MhbP/zjofAzKZtTjxQYjnQYW3g02NSS/DJdUPYCMr3A2AjqD7mDsftoFAcyFn/RZtulXMTN9QOZCIKAKCsMByaQNgsyGPseWrTr656XOBKABiCA/g24dwBI2+tRj1Q/xcobPC3m71RyqSUpnxJlAI8TqPMhPifPYvlNwNgJei9mUoOX1FlQBqpsA0qd0TsGD9ZIK3QhDwCB0FDLWl0h88igMHRsyAnQsHzJdUO+Q0AEJWgz6VEYf+n3f9w/bVLBy1Sz7I55FvIeo2SZrdQAxDWgI6dXsFygGPBL1x9JwA2QinU5Zwi21JJI6WpR+H6X561DSICOABKicBsPYoAj/flxKDndXK9jOA1ADCET/+ocTSs6Ppn/b/GtpzmCUYAbwKIWgYAx6QAs0qDyuEzsaaY5bcBIBB417kE8C5n/Xu9YausBrpA1C+LUqpBm7gL8KTXYnrviefhOvmgKwAwFxyAgAuC/NQOPSTsSFm7w1K3PFlbdVOCPjsuBZjnBwXCV7vvAQCoO+1wYmfMfXCX2ehU+g/a98sr2nlEbXMqoIxyYBCdAzs5olwJjuDqSuMjqgcg4I8ALzltQJ2BNnTypRMApR7ho7/wOWknb19eiutE58DUk+fA2TpfF4Jc2DWMQOUAjIUQWE8cQPo8WjlVe+oAlto35avbRtw8To2Lc7FKYbMAzxoGDBXvxA3YX6c7pGoAIAJI0iUKpwWZg5Zp/htSB6DcGYsE9ExIALUul9aDVywCOHMYcEcl3JrP1AtzXbUAQDAALucAUM3wnG3+HD6reR22FyYhnocHAC4sw7CDwHAdzHnKWxHKAFcxAhUDsOFTwJANiVNx65wCUG8BBqB0tbhSW0O2/qNfl5YDvTAC/HO2+wZuwJp/OBIOq90UADqfApxwTpCV5wDA6a3yD0+YzA9kR0eal2Vg3yw15xzYSW5AJiR2vzQAgspzuXxQN8cBAA9QyhCPIIINALjo8/tRBJh4cidLlx+LBQ5S1VWhSgFgH5D/IoZ/TXIMQG9EPUBdwtkZLwwFR+x04EUhZXgM4KJsksWnhMfVN4dUCsCMz3jvuE9O9+Sv7Pr3GpJyIzPmBYxGw4vqwXZ8HchFfWVrwQgIOHw5AEw+3bnhDMA8zwCMRr2WpCLpOg4ELgJgVnAQ+ORYkDMCZuWHhaoEYMJ5gE+cNbCpAchRABACShqioLBcAGQZz/cxN+ofFgFefMB0xUcCs6ozwhUC4PKo89bAV0lO2/5o2JK2GTbQdUJNwOKMDO4hhmUG4OFytx0aYg+a7klXpfg9NwAApL71PGvgqWxmV0aeVXm3rRI2I2I4PP946OqkLsBPgolUcqz7JQFw+OLXikt6zYQSLR8CrCS+mS0bErI991iOywzAHdHK2K063yduVZsPrAwAm0v7MczdJBx8zjvC2yASjSH9pY1hYzE8tx5saKwN/E85bTxLPvrbVBsKkioVgMsZOitRgK3cc1sXJmk+TQa1AYD2mSZ8op7VBFL8brjob1Xp7JiqAEiteWLen0i+AqDaWaYppJHnYghmxjmPZjgHVN6N0JAUm/D6YPflAODX3OMr31QB5JzdHlEXUKolBBvQa5xZDnxhkyCmJd4I3eX9wECVFv9eDQAB8RmfDyB5bYDMBZR7twa1Ae/nAmCxFFCpN0LzIdK6ytYQcgUFoHFNwdQdXyzyssBEXgzIZKxuG43GWfXg8BzQz1KzFB2+TLarMCFcDQBwEuZgLxU+6NGhDzQHgKYq+cJlL7xo4IxVDA3AgJTrqVkpFbD7UgA4XILT5PMBm9wyMOvXlX1Sxtc0op3TcB42ATyUXLlfplXA+isBwIcAEw51sMS/evlJAOlukOF5hnd6Hke5pAvw41DwkANeV3e9WCUA8HkfkyfdIGojd35Ts9qE+AmyZw7AHSk9Rnnkz0vtKksHVgIAb9940Knr08pdf+kxwAXeDAkNQPkuasAtOkzM978MABtu06+FWkd+DMhOA97oXaGxAZCQrDWvogKqAAD6AGwuB2BwHznfAjQIuc3LQjvqH5YClBKiBNxzMqsqClYAAE+2JxwM9gsswHvzavMSPstnkbvzzoEd+6Ac3mzaXwMAPvGnCJptldcKCv367WsPUM2XtwlrAniQNdOJ9wLmwgSJOgMAWeAd9+fEu6P+QH4S4FZdAEd9GNxJiABf8/S+Xk13oHwAxkLzF5/gcHI7QcIsgHeD62+q5O5uekfkFWsnnN7vVpMMkg+AxSWBdCHFOVHbuRag9yw7D3zms1J/UgXwR+LhnQ03OVjUlvUFYMklgeZinZOoz71cAG7TB1QiA7CraLcElUSC0gHYcUmgiXBHyrKgDjC6zTSQCwZgwF8IKdle7tXST0ZfAQBQZAqXBHJ5lyo/CIR+cOfm1p9+EGoABg9yh3nZ3OMS1EFtARhzWt8XQ9tJQRqwtyDXm5z6gX92T9f/XnZ8OuPmZzhVdAbJBiDlAnKPz6YuQD4A5d/mdbl01AdqAH5K907XnBvoVeEGSgaAj2znYlxjwumcXB+wcXulQA8cgLtBBappwh0TEirn9QTA57KAEzGzMVdbvVEBANbbjQFgMQPwUEHDPh8pVZENJLIf3GHRvZRT6wsT+8Qo8OIU2NJ156G4ocyPE/jRfR7H1ADc3TPNtNxTmYevuKHC/ku/tYH/o38zj78R/Zx7otLQE71va/Jbw+QCYHJ+v58Ka4P8E4EMgOByxXOBBLkR4N0d61M946VPUxtKqnb6VGcA+EUnqSdrZS5w6B3GN17o+lBlsw2l1Wptt8mfW8mfeOG/t91mDP2aJAZgz1pJQ/kBQool/tvTmkceuU2zl39YnMi2ALscZQDyBCczigC4MNZas5nQ/G3TvePlV6YSFUAKcMAOAtvkT386mA5i6ffD/49kwEv05cm5Y4vbKbp0GyAVgEfOifFT2swr6geGjuBOCQCcKY33dgqATWQAQJfN4HLQ6SAr/f4gIYAjoX86AF1ObfrC1cq1A4C3AGmWaRTYKwRgXAIADfF6mCOE/XwGAFsnP+n6/4Dv7qEhLBeAUA/wX07hBwGKUwFYc96yKd0GEMkWYMJ9ko3oWG2LLvVtXVpvSzTA8ATtPxz+GoYAbMTc3P3d3eBfwNcmD/1+uKz8Foev0jbgYBWmp5cPLc5vJLJtgEwA1uJVAGJ2Z5x/KhyWrHVp8AtOIPXn2u1mo9F7P2r13997jedmu03/mTj2bcMiwJ9MlXXVP/d3GYPfT1Y7sQH9u/v7h4eH0Ak88eN0hekZqmbXFQCHS/1Z6axmJz8PNCwDAHa+NhTSajbej1j+5/b28G8sI2MAQk+ehYAP9/zyR39M1h80f396/0DODQPDjAkfB2zqCsAkGQS2ztiybn4eaMhudb/4I2+6vjLbrXS4nYi0P0QAfuMzXEmtkWDmO+PxPG0Aft49RPS6M1hXQhEIIwFw/WItcDD9dPnhpzRrpyhOJzexdHzwJJQHawYA39LiZDIafhEAvTIAOLyHjc/uji72BYYLdicpCTp5jV5LagB+3t1zrvijQj/WQ+gK9hMAOItwB7/R39sXqU6+h8KqKQAbbtcHmfyaUpAJ7pUKANu2NABpF/uCwwXd/kF+990TAQPwUzyo6O1CAkLrzwjoJ5EfrD/xL+vkWXOH6R3JrYESAeBawOF4+DgLwGJUAEC5XXfs0tAiKwD9R4X9J0FkAJzMDqXhYGT/+6kUwINKLi7iW2IXzbieAHBBIDQGejkaYCQnD5DH4nOR/9csXn83MgCTnNe764vhXpz3uy/jikOFswGW3HEh8gBIBYHW6wkmoFu+NtoWGIEPrqTwVPLzJzUAOa2ZVnhVVAaA/pSU0c624WrCM7lHhOQB4CYf4s3Ktrb4xQBI6An1tPz+Myg9Ft5JZKk/f4IB2OStEDsjxq9/P1IAZawW+M8xR3O5jWHyAJglagw+TzqhWZAHgDWR0RQ8K8g7UXUTFCay/6Xr/29+Jk6HQ2Kh7Q//LwwH+n/KCdomiRMAD69bRwC4YHaeM/pwXwxAU4LRy+9BZ8OoCjxOU/1BFcDPgt78GZsT0U+ngaclzbbiA8GV1EyANAB4LTbLiWXXRbUAdi6k/JYwouYDUDSVE1KAf/38+aOADze8LSRlAvp3JTVwmKlm6qf6ATAXboTKMvzEJrYXNYWW/35yTyIDbAVplp36718///q3yBqtVTYsOOsClPRA9ST420i9WVwaAFz1x8txAV4NGA5RBICEI1EztZ3tQAN/IyjY4T/++uuvn4UunRcGgtkgsKQTvTvRCfDrBwDnxsyzWQCW5C7qCJIyIqwg6ihwONcQAf71s/iNwEGh/jQnC1BS8dbhrKbUTIA0ALhTIEpuOntV2BM43EpognDyw4B2voNlQQTw86F459lJGMAD8FDWXl1yfvNM5vEAWQC8cGXMSa4bGxQCICMTlN9/MCoYSKmwCODfj8owDIBo7RMnsDQAoAq4Obx1iceEZQHgJjWMJy032dotzARdfqVvrlO6PXoi6R4MwM+fH40CerMSABIlUB4A/K6R2hcmCwAum23mp7I26nZYoAGaEiqgm7wexGHurQRUvf9fGgH8+Cip+7YqAKCsc82zpJJia6q849KyAODOgTn5GuyxKA4EL7B8lbfPBaCXBwBEgH/RCPAjzysEIB0HlgjAmCsBWBIbA2UBQBINNitoaSC5g8LZ+XAJXuC6CICX/Ajwr49HFRYA8Ke0QqbJ9QHMJDaFSAIA7sJzk1TmriA1kw8AXOVTeuBrFAGQzvR5NMCD9f84pysdAL5+MpYYBkgCYMnZfb0gj9GFG3yLnIDShwR5RQBsshEgXf+HTwpSWR8gqgV1ynrD3GMzJRYEJQHgJnbf0Ar6Wje5F4aGTkD5cU+hBlimsfzBHIBPqrqFGqA0AAIuFyixNVgSAFzuYlOE7xNRiw5wjbalf2CqAUZHAEBdBeYAfJZ9z4kCyjUBQhMN1QadegFA8Z28JWmMp9cCJ6C4J0QpH4BFTkNoGgBL/Yeu/49PfZBwaqxEHwCU6EELTeTNipEEANcC5Be6sOAE5DdqDcu/MmidC8AoBYCvPjAD8KnTHY0NlgiAKY5YntQKALD744TeoOgj5k8JCutBJVdAX44BwIwMwOdXwhcC0CnzGc65TfRWJwDWXAnbKi5mkiIbIKEtjK7t8FMASGQAPs/myHcCwfA7WZ+6HgCA+lonn2Nc6Oe0io5rlH5pUO5x9BQAoQF4OEbd8j7A4YRguQBYYhz4WCcA5gmx3geVjMfiQQ6NEu9lPRaAJTMA/xx1dfPBBAz6A0kA7BLD/0ikJQLkAOAnx0KXHyUxiu4MYhXBXRUAcL6GrTMDQI46lxQBwI6FJWeDy3QCee8Z/IFlnQCgXmvc17n5yHx11O1iWE0uKB+AHgeAEhmAo8DjNEA/nhFRbhQgdAbr0uqBcgCYJZ6/+1Ee2yu6NaoHUwL8CgBITMCGpQD/OdL3OCSCBofzgSWHgczzi97LmyWtNVwKAG+TY9IAYcKo/V4AQMluYMFMogMANiF/MwNw3FFc+ZlAoR64kpYJkqMB9ARYJckJ5kfnBW7gkLqBY8kALDgNsDs6AswAMOCLQeW9Z2im3idPcVcjAKg7deiz2H3MbmEqYDhsq6Q6AGIDcGwV8pAHGEjTAHxboCKtMVgKAOC0ukki0P/Q09kuirvDN7IBiGsBnhYaAOK9nq4BuOkAZQIAetRNAHioGQCbxHp91OJrfxAJtsvsCsh3AuO7KQL137/+/uvH8ZMpIg0wGAgTw8oFwEoeXVdVSY0A8MRMsPNxtEsWi0IVsJcLQOwEQhfY39QAHK9mM05g+QDwh+rH0oYESAFgzfmv+icTTp4+VAGWfAA2oQGgckoramFHUIkaYCIE05KKAVIA4I618O5AkRfwUU1wXB6UmWIQYLcFqw9NAGAATuhETRWDBjIACJJccM0A2IsAzD8LGT7oCylN8WUBGI1GNNZ0WBfY33//9e9JgVYVTqCS3B4zrxcAqVrQJ2WM8Ye5gLLSgS+ZKUEwG7plsd5Eqv//Oe0JCwAM5AAwSwDY1AsALol5BABUA7eLS0JlXdRrpgFYjEb05U14zP+AA3DaUYQIgHhIzKAvVwNQAEg9AVgfMefQLBoV8OEIn9MBSCUBFsOGqoC2eqAOwMOJqVYhEzjgwsASazZOAsAy74T97QIwTq45OAYA6u0UDXH7YIbPhRpgRAHYkldDjyLAE52NFABJGCgPgHWtADhFA4Cn2PxVeFC0HCOQ1gDUA2yqy1df1cIa0In9FgkAg6gnICoHlwtAq7Ya4ACAph6RXnVhkmd+SaCxLachNj2UCibR+PD2wgjw1HU7+ACHKwPK9wGcpBXUrDMAx7zzgF3zlHtO8LmcXggRABhSSwjkWiAC/Od0xpKGEGFqPAKQB8ARxssgRdPjF1RTl9EPJwAAyec2fV8dFgH+rZ1uZeKWsHhA5AAByAfAOw4AMHK5Z4Vh0H9b1Y1SAViwCMCFQAsiwB9n+JmxBoibgtAE8NLhEkHakfu3qxZdJg7znC8vC/JRwGgxXBAILwMaAfz9zzmRZronUIYG6CYA1MwJdJNUsHd0P+ukyA1gsaBSMgAtiFPnLAV41jiSpCt4MJCVCvZrmweYiwAcV9OlbkCrIBR8pwTMLgdAGBC6hG/98xtSgOe0nSRHwwaiD1BiGKjUNhW8F0eEHWlhgYCCfND788VNkRwAo96IKPDWaATwP8p5yoUBME1dF1IyANycqJoVg7h+VvuEjvZHCAUKe4QvJGAZAQBxxa8WYaEnqwGd13KQAHAoCJUOwK625eC1OCj66GdCOW/3cqNBFgwqZQAQvtYanmlYA1qfD8AgXnopJ4P4m7ZqBgDn+b2dAgB8zvYwJxZY0GiwSdSJcYlWipxAiAB9KFP+YBHgmTtWPB4+SAAoTwOkWsLqBADfBTI5adQ1DSBbi/xo8P2ZqLp5AQBR7+Gwt4UPvQqbAFrnvl52PkDZAPBT9p169QQ+EfVwtjc4zXgDAQXRIMQCZHwRAAtqYH41obIepoDI+YWmCgDQk6foSxsUJ+1kEHcw5CTbTa3AtjAfAFc8nrlkcKP4AnRAA9bIjQwADVD2jpuRlxf3Yxk7esoJLB8A8J/n2YCgFgCIyus0P3upqeT5vbA4rGpnKQFPCQEYDgl0gWmgAFgNaKOeK7IBMLgUSlCvo2Gw7YPEsz9Rea0p+c1egRKgjoA6OXlYhu1r4F4CAE31EcLUHywF6L16hPx5eHj4k5acbwl/+/Dn4T57aVC5ZwPVxASs6nU6mNdYy6QwdDT6pCgcpFF8A256Vk7ziOH24O0zDQDDLrAnnRUBWc/5ivTPF/FsYNlh4J47Hk5qdjoYJoREZYyXM5qZ4LLfbWO4yBvsN+wBAppy9GvaYwvuDw/bwIeQ+J+p5J/fv9vAaIfd/DNlMjhaop9Oa4CSAeiIAyL8OgHARS1PRxcDUh8e7nwv6BV+ps6gGhxVYzJnOrxUo0dfawQXRb+A1W///s1GAXnkUNEZ8Dn9Y6Uv0wQ4yXlA4/iE+m0AwN94c96Y073O4sGC0kCPIWA5n0QEngM/RtqNd7r4ixGNAME1scAA/GbNpi3SFxb9QEO/cMVzCBjIOR7OXbX0qNZsSNTjJ5cGHuMIzJjiLrj1/dfwuU3totrylwX3Pq7nigW+eqvZ+BV1gYQXUfjUAPz9mw2kn6v/r5+n+aPBXwU7Pg1EX9KQKO7SmGXdxsSBynpJPsd5IewLXcBts1dUHho2mowBzQr8zXJ9cDQN88VVAp1FattmYxG7EgsYOzSHmwz+E9eAyEP/ROsvOAKDfl+mD1DnQZGfXxp3lHS1D+wAjBdrPLe3UVSu68SiQnQt+gZpweoPF7ErCW3gdEdNyKEG1L0kAqDLPeX6AfpxW3hpcwL5Qrq8RKAsACblpLHXgRohMCpyB0ARtLaEJAkaQratdrPRGELcx0USDQDRVWkA8PsHbNQxuev37y6QUAfENqDsQZH8sOidWv4NGnIB4G65mV90qGU5YXa86OBQ1Dbaa1B5ZgJ/GopDyMOv3ltUixoQAPz+DxglB5L5U8Kjc7zAP0tnAksGgG8Casm7O1QSANyQQ+/CKZcvq9CYFxIwGnEsxGs/EhFZwEHQPRgAuv4deLImu/99SlSNhKKHEv83+YIIPxD/gSQdQbEvWC4AXBTI11ZqAsBL0sRo65fGsMsgDOZ6H8ooluzfhG3gPryt/1AAiLaG2+Hv+lMAYG8/2afKqx1XAw/RQemTQndJ+t+r35Uxa+4tlzDl8tHXw4iuKCz8WKhWaEAX2Eb98RtSgA48XrqBp3d3Z99TPUmOhvV5DVAaAK3Ej97X79IoPoYppZRpu4oWMtA4bfHf3997jWYLFtpUQwNg2TAXmO5aSsDZF1P8OHQFc5MiSwTAJkkx2ElOW9cGAO6yy7K6WTyHZXYIePiNX7/eww5PoWAwYqZgsQjtwHu4+GGk6LNp4GAAiAdDSe7Ykp0NwNuP1PHwsgFYc7mfGl4cCRbMSuKZks62v62dlRZC0M5TBZH9j/xAGhfQCJHligLnER5jmxkA99Uk8epNywAgXv1SAYAh208HbVO/q2O5bV9uJcNwZ1aY62HhfhPiPj7eG7KIsNkOl54u/srfG2E6FQzAP+CPgAGY9i8DAI6HpypBpQLg1/zy6A237a2ya5nGphsQjUv8UGkxoX/gIns98DdxKE0NQIc6AFANoBHANMriTM91AjOXRpXtBHK9lLW8Pp7f9jsp7SzefjwLVrqWTeQQTV8pM2fzaAsP9D+/n3+zUVABiwDYpi0PgLJ9AC547qiljcqqDoB0T7Ok3/L2anhrc+mOnW634zhOZ2+aa894zQyoH6vt/z7/ZneRdVgKCBZsWiYALBwsDQD+umBF4t3R8gCYiV6g+XpVoRHgr9//fYY7TD1CkoMcJZqAck8H85vGkndtpEQAnCSVbX82L1i+WOrzr//zi3UBTtSfZQAg+caQnegDKvUDwOQmhku8+vZYl7r969evNiTU9+AAHJauBAAGUm4N4xJpS5k+oDwAwAv0E21gXXP9aQRI1/+ZVdSoAQAHMJrscRkAU2kNIfzxWkfacAipAPCdQKZMN/YIFnVqAH79YpNAFPUuBKAMEzAVLg0qEwD+rjVF6vaRB4DC1bO5U25XkB01AA1qAPaQnuCWbVCSD5BcG1IaAFwpEB5eUEcA9lxBcCf1M3wic7VF178JbohH/gj52wsA+JE9HFoeAE9c7g+cKbeOADxxTsD49ONBJTojpNGgDgBkpgmZDkoCYCUTAH40cFfamGDJAIATYHE+zf5KAKzUJlUALQhJlEz69kbDQL4JdCLXg5YIgCOeDvGvs/6O2m40wgjQV/+kkne3CgB3WxAkUWb1BGDNGa+ZvLbWz9IRBKqD8NtfVDKVCUB5UQBfSJGbBZAKwCs342YjNZb96POpjfdGg4RXQ931xfN8FwMg6Xg4N2gTrIGsMyHSAeCKGOARjq+w/jO1+d54hyYQ6kxlvbZSq4HlaQD+GIDUQoBkAPZcNngn+XPky0ZtwWwhpoh0dVomABJNAN8AAg3B3boCYHNjDdxr2ABDZ/eRbfXQD73L6GwJAJRwLoC3AK7sSqpMAKCrRef8GqdqAAI2gr4dPkFSGwB4CxDI7AWQDgCPb1B9QagDY4GGz1EAmgFgUAIAg4wTeDkAoDi7OUq0hgDwFUFXbkIrRzyNNIbDBtFfEwD6pQEwyUQBZR0N4w9TztXLztVdGQBQZoTzbKrNBa3UZxYBmHEu6q70KGCQvTz6cgCC5LIwyJ/ItQCSAdiIcUClNqALNcD354PnIQAQ3fIAALxcAEBfAgC8twRpQKXOALxyecx9tZ2Be3ULh8bJ5DUHgEF8pq/ERFBZPYFjLgaQbwFkAzATmwKUytbf09hNhK3kxt0UAOU4gRIAaHEZE+kxgHQA9molc07yHIB2r9Frcmn0BIB42FfJqeByAOArKJAFUuoNAGz7mGePSO1sEERRW70RjQCV19fXXB+gfA1QUi1A4Y7SOhVYTdkA+FwGcFJZSfBF3cKJQaK/FgAgpRZQCgB84XxVgd8sGwA+l12dG2ipjfdREgHWCIA5t2GgGcypOwCw7Q+/g0g85iyqUagBPotP71ALGNwyALwLOKuifiIdAD6SAZtmyF9/qAGOeg1ivb7WTQPwHaCQOpPfSisdAJvrajYqyQZCF2ivMWylMs/SASgjCthx3bMdiaOhKgQA3NqDIptJu/tINAB0/Ztp81kHDWDwxwCtSsJm+QB4arLt1xW4NWO1NYSpcKvXLADTwW1rAJ+rA5lqJcUT+QCwdJad82c5stbY3UBb4lUNQP/im0PBXO44F7CK83QVADDnHBv5kY2lNoeNYTubciIhAHxbqAwNcMmn4xM/0EVZRchUAQCQ27C4qFCqCvDVdm80bOR0UcQNIdxNH4NyJ4RcCoDNpU0ZDMsvAoDDubPQ5j6W96vCO8JHJMd74gDo3yYAY37N9Yqq51UAAAHt4cOsZPq2NlHh0sl23qpyHUGDgbyGkAsA0LlUuatWVDipAgDm3C4PWRqJKmAGEWCvmes9Jz2BAwkzggaDiwFw+bh/VUXAXBkABp/TkhjezlkNMJ0CjEST0BOYAeDhAgAIpydN2ccBqgWAqYBHzgtwJHFGGj0aAeY2n9oamQrjPGINcMGk0PSo2CkFQLlEAcy50Lmi8/TVACCogImsDzehBgDuhs3dOkbmaCjTAeRcS2vxAETXzAEAZ+du9JQCqKiDthoAWD6YT3HJ6HNx2DGARkHPQQhA6nDooP/jXG3EZZaT2wQvAIAPlZgCML4UAB6vAnZSypxwEHy46JGCJxceDh+khjuebbRtAEBsMYbX/vfcYxygIye8Aqhqrl5FADAVEFtbqA6UXuekK/LMmkAKDtNvVCI6APFZrvO2rEFii5JcGUO/vj/3CKzCN8tMKjxEUxUAoAJWvE9YdmsQHATvvT8Xpk+d9HiQEICHM/Ota5Vww8YOL3d/ZvZG0JDgJu9evxgAbNHjzflUfpprSSPARqNHyFPhFnuAa2LTkeC5K+ZGPMVeRaQE7lTtrJcL+D1vVTlWsTIADD4dOC87zLV1QpefGoDCoO4HmxCYOR58f6a3xeZNZV5ucGZeYc/7xRu1ysm6lQHAVIDLJbpK9QOhC3BUkAKM+buD635FBcCuDTsrDtQz82b6F8SBOu/1/6guBKgUAFABOu/nlugHzqMaoPUBfnBR9ODgsEcAwIpNztqypJ/d/wPQKPp5e6PDa8cKT9FWBwArdjmJz1bi8CtPI6PeorctPuhJnax7bsW4e75+ntV4Z3GlIPG/5PQch8EngZkHYH9JANhHMzh9UOiwnf7CxTWg8LdZ4LMNknXn9u6DSk4Nud4UpgCSLJDgVJxc6xJCwLFa7SiVKgEQ9L5bnqvjq+1ho9dITtVnfrEO9wQO+tk4MLwyQDtNBxh0xfhpI+KrPpya51zyTwICpCrXpFIARL2/K6vr2VS3iwYYgALPaU1/LbmL7nQRKAiTN3dwXuX4ktDa11TCIoocmsJ0sEqc47046AM63BDItMH+ywLA/ECbY72UkreuNlgXoKh54Y5nwzDHClw3+ueOhYCi7o9S+NP+9AGumNs5rukZtv2avXEqfDnD8DZj36K4PNxlI8pDIDjt39Mf0Sy/s/eMJ9s+ZlfsJTnHtwYAq3govLtbghFQoAt00RSem72DC9618Eo58nDfDyMAEYE4jzOlSuCBRBfOafQfTjIB6kzXtejVVPIHXm6QD0D4qtP7H+EFduzl9A8/5EYojVmVT9OrFgD2AU3eCFwcCeyhCWTRICS9q9iVkuTPw/00ueT9YAb4Gg77m+n9vw+HGyetHG7Dl3t4uL87/Jvsyid/urv/k9xg+UFU90R4p3+sVj5Uu2IAQMUR3vpdmvJgTSCjXktMAdIo7Y7JNJWtj2r36eg9/M40/Dd3hBiZtF/ycoNk9w8E289/OQ3vJmfy8JFSFzaBYCK/JgDMyfF5//fCmgA0gTTemynH21LvIoM8TXQ/f79fqpVrEP9oWCBKAzCL7wkeiC8Xr3qIEN9vPohfjcr9B5UdV7D5geTJ4LcAAOz6xMr5l6q8MaQAe5kUYDQYfBoD0P9UBv0EFpI2w7tDFmkwGBz1cnGPEPiYHwAARcCEto16hZt1qgaA1T1Wgk9wQSzoqaQxGvYyXYBW1K0xnR61WoI6uOsTkgPAWQIIUgAKvcAVfxKAWUTvywPAjMChErjWLvrQKxoBLmgEmB7OR6jrT+XHjwdRfhxE+CYRfzCjAYLw5eIfj1+I/Yf7PanXZS9F/cbi5LCoABX1CuOUrwAAMwImbwTPdgO6MAx6+JztAhyrF0mQE6pdIJpZnAK0BP/4ChP1qweAfe7E1/VVcm42wISD4DQCzIkk9g7IeNyhMubESUT8JvvR8E9Otj5sRj+dfrn4BVJfd2JhL+ysi9NiyVt/0q9zu+YVAGCaL+D8+DM1H31kjV6j167ccS4xJ7IXHoP7+j0AgDMwyYc19DPPwYbnwJ4rHD9aqsyo6usKRusqH+QqADyqvPIDk3CG8otHQen1XP+OEA15WqVtQNcGIOX6Af3WqQmwQwrQrOX67wVHiJmD61iy6wDAkvWzV94RPNUBnrBJIM1r3Ud5oayJ0Bk/q7QP9BYAgDZIzudRKAGndcJ31HZv8WEX4C0LuPzcjnfVirtAbgAAVhV6FFSgctI/h0kgve11bqO8WMTIB6ZoV3ydzvUBYNTrB7cHLkc6QZvbejQJxKnl+iti7sPim4K/DQDM8Itu8PFxMESAqWnwtVp/Idm4u1YEeGUAqB7k9wEEg8c6wvPiFGANxBez3+PrpIBvAACWAXK4VT22MhhOAhnWNAXoCLaP1RmqbgK5EQBYPohbcvdYApSoCWRW1/XXHsVncM1UxjUBYEvOPQxGwOc90Z2wCYTUMgIcix85pQW/GwBpdege4wd4ZDsaQQrwsYbr3xGrw7Z1vQzQTQDAHGJLVAmfxQIW1ACpA+DXdf+bYj5g9/qdAWB9kKvXE3SAH9UASQ3X30mt/0yt7BqtmwWAtcVNxD3ykQ6ASSAwDLiOBoCtP1f47l45ALgNAFgeLEgRUOgWPelQA3yvZQoQ4n81tf7k6qns6wPACiOcIWT5gG6hxYAUYPOamZNL3B1+/6fiwe8LQBgKcQSwFsxZYQS4GNYyBRik7H9HrehCgNsH4NXTxRVfagXZUY9EKUC3bstvgKHT1ylLt39FAKKFFQlgj8vKmscJqwHWMAW4BMQnhqj/b2H/3wgA7HwIv6z2Kq+d3olSgLXrApynDxu4t7L/bwWAV1NLpUQm2YSAp24bi0UNuwDHaafm6KrH9wHgda2nNomSOVgPKUCYBl83AzBLfxLnhtb/ZgAIY4FJet8EfBgNsyAbas0MAPNnBF3m34z9vykAQgJWXGJso/F9g8u4C3BZq/V/gQ+hm2nVdjsf4oZS6k/pUCncPOPQLWRdgO91awNnuz3gqLYnqXwQAsDJKv1w2G7Z2a9RCrDXuHrt5LTwdpLOazOmyS3VMW6rqBakHxhzBPQNpAAbYABIndrAmQ0ThlAyV1e/qc9wY1VVtuX5FlmTPbIXjU2Db9YqBci8fyGdxYho3VYa+9bK6k7GaFImzJX63Gi8N+pUAzKtTBzLPtvuxt7nzfVVhEqfPyfTcaMuwG2NakDM+xO8/1Aj3NxJhttrrFmStOUMU4B1agMPt7/CN3sw9/8GY5gb7Kwy0srTYqOgnqsfoXam2GyvawKuzJfRbpDgm2ytUwT3qQspQGgCMFedOqy/qzNbL5irLvtEt2jBbrO3ssNtoUe1FR0EpRtrcvOtgF6Q3f7G5Bbdv1sGINSYYdmHsEkgDVUx2cy12U07gravZbf/XlevffyjdgC8GmwjWUvWBr4YjmB444Y5B1r3Zpf/bcyW2trkBAS3WsK43fZ6Gg8SdWdCG/hoGLUBj9kAdv1GXYFlCKi41T32zcnN6q0bPl9BYynNY23gvec4BWQz/1Bt3eB+MoMcE/U21tSyL8n8NgDQaMBVWA2ox03wX+/Yc15tbmz5QzJXYrfSesVMwi23MN32Cau92losFqkawHKi5ljaq8pjkPuWHO02kz+1AQAmQSwgBZSKoObWTWkBM7yhRk/5+Y+rm/b+6gDADqaBwygYW0lpUTe8jmd1A9XBZZC7/KHzL6aDEYDThN0IDDWApUsVaepJuqEW0LvX9a8PbyP19uZ6FMa+IgDnikcNwIgNA/XYM3ZT9/nNV+E0/tnVsoOGo0e7387LYtSife2GAViFXWCEvD6F6jSTB96E7qA6uUqRZalo4W1y49Ty213t5p3/GgAA54AWMAoGegPMcKkVr2ARdMWsevNbRfCFakEfv74iAJe41uq2N4KT4A7v9mlK2uR73WgliF/ZtFXbDULuNCVjfjps+TW/Lr0rtwqAHV4HwneBhVG15mf86k20HOpkXMVj3+yiX2dlr4jehzQG9eldvVUA2DkgagD4NmBjlu9ygRrQo/uZJmOpz96YK9Fv0mfLzCXTm9Attep0duVWAZio7cYw0wYc5YFJjoJdzvT45l9fzgK8rZ1JtPfViftUFBES9+0VAbjcBdBV0mxkmyiikkuejbXnsSlQ9Z1bsiJ42iiRr6GSiZN98bcoO1kX3+/2nUDWWJd3EDQKCHLD/6d5bJ9VreXvjVL24pux7B62foGNKcoHIQCXKAFLz4/tIi2QH/7bm4MtUDVrNn68iALDHM+sePFVPchVLJ6vFzknCIAcWc+i8D8/2jK7iq7yFCzPMAje0uHWnm59f5m3vm/LSOlY41o+ynoCALsujsTzPT57qUwId3srmcyc+fKIKPGN7vq5M5voGvevLWWT/0+fxtY1k5HfGADIxZE4HC/Y309LJdDFW3x1a7XznbE7N188z0jE89Z02R1/NrH4lQeXL+gWcrOMFJG6M99eEYDqJa4FaKviDNB67gepNY1pECTn763A+cB2rOMUpO57NX6GtQYAOrHiDNBs+aEv5/qBlc9BHhqW4o/3H9kL250cMgL1foI1B4BfCn22/8QL98yNo+wmKz13w1P7oMx86ims7U/CwvkuTgcqj3V/frUHgDmEcY5GC45LAD0Z3vrRXC73oSyX5pp6BK/HGHLzkA7Udhu7/g/vKwAAy5IwMHGkVYbt/SEdqK0c40s8uS8CAO8OgGZ2S/fL7GVnoqvVlJwQgDNledig4Mc5j2VpaGPZTeJJTXGNL/TMvhQA4A90gsTB0xV/c+FWtU13ttI+zwghADeUIdoofArX2nU75nEOnuDr09dxFItLJOmB4329p/UFAYDV8+Y8BJACnMx81/QM+zMQ3mzDM11/JmYNdKW7tL/ko/qaAIThuunMJlo61m9NAkVRxu5mQ4M/cw1imuZys5m7jrILIBVMxH+zmrlr+8s+pq8LQGzDnZmlq2cJVRvKeP309qUf0FcHIHINN66vnMABsSaKu/G+w6P5HgBE1h0qfv4sCCYkpyxAzQOh5sF33OWj/fZtnso3AoD38G1WAV6vl0v6f4+P67XnGU/2d3wW3xIAFAQABQFAQQBQEAAEAAUBQEEAUBAAFAQABQFAQQBQEAAUBAAFAUBBAFAQABQEAAUBQEEAUBAAFAQABQFAQQBQEAAUBAAFAUBBAFAQABQEAAUBQEEAUBAAFAQABQFAQQBQEAAUBAAFAUBBAFAQABQEAAUBQEEAUBAAFAQABQFAQQBQEAAUBAAFAUBBAFAQABQEAAUBQEEAUBAAFAQABQFAQQBQEAAUBAAFAUBBAFAQABQEAAUBQEEAUBAAFAQABQFAQQBQEAAUBAAFAUBBAFAQABQEAAUBQEEAUBAAFAQABQFAQQBQEAAUBAAFAUBJ5H8BlecQzyneUwIAAAAASUVORK5CYII=";
const ICON_180 = "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";
function sendPng(res, b64){ res.set('Content-Type','image/png').set('Cache-Control','public, max-age=604800').send(Buffer.from(b64,'base64')); }
app.get('/icon-192.png', (req,res)=> sendPng(res, ICON_192));
app.get('/icon-512.png', (req,res)=> sendPng(res, ICON_512));
app.get('/apple-touch-icon.png', (req,res)=> sendPng(res, ICON_180));
app.get('/manifest.webmanifest', (req,res)=>{ res.set('Content-Type','application/manifest+json').json({ name:'GAMServices Holding', short_name:'GAMServices', description:'Console Mobile Money — GAMServices Holding', start_url:'/', scope:'/', display:'standalone', background_color:'#16224A', theme_color:'#16224A', orientation:'portrait-primary', icons:[{src:'/icon-192.png',sizes:'192x192',type:'image/png',purpose:'any maskable'},{src:'/icon-512.png',sizes:'512x512',type:'image/png',purpose:'any maskable'}] }); });
app.get('/sw.js', (req, res) => { res.set('Content-Type', 'application/javascript').set('Cache-Control', 'no-cache, no-store, must-revalidate').send("self.addEventListener('install', function(e){ self.skipWaiting(); });\nself.addEventListener('activate', function(e){ e.waitUntil((async function(){\n  try { var keys = await caches.keys(); await Promise.all(keys.map(function(k){ return caches.delete(k); })); } catch(_){}\n  try { await self.clients.claim(); } catch(_){}\n})()); });\nself.addEventListener('fetch', function(e){\n  // Passe-plat reseau uniquement : on ne sert aucun cache pour ne jamais afficher une version cassee.\n});\n"); });

// A partir d'ici, connexion requise
app.use(requireAuth);

app.get('/me', wrap(async (req, res) => {
  const u = req.user;
  res.json({ id: u.id, username: u.username, role: u.role, nom: u.nom, linked_id: u.linked_id, mustChangePassword: !!u.must_change_password });
}));

app.post('/logout', wrap(async (req, res) => {
  const token = getToken(req);
  if (token) await pool.query('DELETE FROM sessions WHERE token=$1', [token]);
  res.json({ ok: true });
}));

// L'utilisateur connecté définit son propre nouveau mot de passe (1ère connexion ou après réinitialisation)
app.post('/change-password', wrap(async (req, res) => {
  const { newPassword } = req.body;
  if (!newPassword || String(newPassword).length < 4)
    return res.status(400).json({ error: 'Le nouveau mot de passe doit contenir au moins 4 caractères' });
  await pool.query('UPDATE users SET pass_hash=$1, must_change_password=FALSE, failed_attempts=0, locked_until=NULL WHERE id=$2',
    [hashPass(newPassword), req.user.id]);
  res.json({ ok: true });
}));

// L'agent envoie sa position GPS (temps réel). Diffusée en direct aux superviseurs.
app.post('/location', wrap(async (req, res) => {
  const lat = Number(req.body.lat), lng = Number(req.body.lng);
  const acc = (req.body.accuracy != null) ? Number(req.body.accuracy) : null;
  if (!isFinite(lat) || !isFinite(lng)) return res.status(400).json({ error: 'Coordonnées invalides' });
  const at = new Date().toISOString();
  await pool.query('UPDATE users SET last_lat=$1, last_lng=$2, last_accuracy=$3, loc_updated_at=now() WHERE id=$4',
    [lat, lng, acc, req.user.id]);
  await pool.query('INSERT INTO positions (user_id, lat, lng, accuracy) VALUES ($1,$2,$3,$4)',
    [req.user.id, lat, lng, acc]);
  broadcastToSupervisors({ type: 'location', userId: req.user.id, lat, lng, accuracy: acc, at });
  res.json({ ok: true });
}));

// Déplacements d'un agent (historique de positions) — pour le superviseur
app.get('/users/:id/positions', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const r = await pool.query('SELECT lat, lng, accuracy, created_at FROM positions WHERE user_id=$1 ORDER BY created_at DESC LIMIT 100', [req.params.id]);
  res.json(r.rows);
}));

/* ---- ZONES (SUPERVISEUR uniquement) ---- */
app.get('/zones', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT z.id, z.nom, z.quartiers, z.responsable_id, z.suppleant_id, z.created_at,
      r.nom AS resp_nom, r.prenoms AS resp_prenoms, r.username AS resp_username,
      s.nom AS supp_nom, s.prenoms AS supp_prenoms, s.username AS supp_username
    FROM zones z
    LEFT JOIN users r ON r.id = z.responsable_id
    LEFT JOIN users s ON s.id = z.suppleant_id
    ORDER BY z.nom`);
  res.json(r.rows);
}));

app.post('/zones', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const { nom, responsable_id, suppleant_id, quartiers } = req.body;
  if (!nom) return res.status(400).json({ error: 'Le nom de la zone est obligatoire' });
  const r = await pool.query(
    'INSERT INTO zones (nom, responsable_id, suppleant_id, quartiers) VALUES ($1,$2,$3,$4) RETURNING id',
    [nom, responsable_id || null, suppleant_id || null, quartiers || null]);
  res.json({ ok: true, id: r.rows[0].id });
}));

app.put('/zones/:id', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const { nom, responsable_id, suppleant_id, quartiers } = req.body;
  if (!nom) return res.status(400).json({ error: 'Le nom de la zone est obligatoire' });
  await pool.query(
    'UPDATE zones SET nom=$1, responsable_id=$2, suppleant_id=$3, quartiers=$4 WHERE id=$5',
    [nom, responsable_id || null, suppleant_id || null, quartiers || null, req.params.id]);
  res.json({ ok: true });
}));

app.delete('/zones/:id', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  await pool.query('DELETE FROM zones WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
}));

/* =====================================================================
   FONDS & RECHARGES UV — LOGIQUE MÉTIER "MASTER"
   Un Master vend/recharge des UV à n'importe quel PDV (peu importe zone/quartier).
   Le PDV appelle le Master au téléphone ; le Master enregistre la recharge :
     - son solde UV diminue immédiatement
     - le PDV paie tout de suite OU plus tard dans la même journée
     - si payé plus tard : la recharge reste "EN_ATTENTE" jusqu'au règlement,
       et si elle n'est toujours pas payée le jour suivant, une alerte part
       (le Master ET le superviseur sont notifiés en direct) — sans jamais
       se fermer toute seule : elle reste en attente jusqu'au paiement réel.
   Le fonds de départ du Master (UV et FCFA) est crédité manuellement par
   le superviseur (aucune génération automatique de fonds).
   ===================================================================== */
const numOrNaN = (v) => { const n = Number(v); return isFinite(n) ? n : NaN; };

// Le Master consulte son propre fonds (solde UV disponible, solde FCFA encaissé, fonds total)
app.get('/me/fund', requireRole('MASTER'), wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT u.solde_uv, u.solde_fcfa,
      COALESCE((SELECT SUM(uv+fcfa) FROM fund_credits WHERE master_id=u.id), 0) AS total_credite
    FROM users u WHERE u.id=$1`, [req.user.id]);
  const row = r.rows[0] || { solde_uv: 0, solde_fcfa: 0, total_credite: 0 };
  row.solde_uv = Number(row.solde_uv || 0);
  row.solde_fcfa = Number(row.solde_fcfa || 0);
  row.total_credite = Number(row.total_credite || 0);
  row.fonds_total = row.solde_uv + row.solde_fcfa;
  row.en_circulation = row.total_credite - row.fonds_total; // argent chez des PDV, pas encore payé
  res.json(row);
}));

// Liste des PDV disponibles pour une recharge (aucune restriction de zone/quartier)
app.get('/pdvs', requireRole('MASTER', 'SUPERVISEUR'), wrap(async (req, res) => {
  const r = await pool.query(
    `SELECT id, username, nom_commercial, ville, quartier FROM users
     WHERE role='PDV' AND actif=TRUE ORDER BY nom_commercial NULLS LAST, username`);
  res.json(r.rows);
}));

// Historique des recharges effectuées par le Master connecté
app.get('/master/recharges', requireRole('MASTER'), wrap(async (req, res) => {
  const r = await pool.query(
    `SELECT rc.*, u.nom_commercial, u.username AS pdv_username
     FROM uv_recharges rc JOIN users u ON u.id = rc.pdv_id
     WHERE rc.master_id=$1 ORDER BY rc.created_at DESC LIMIT 300`, [req.user.id]);
  res.json(r.rows);
}));

// Le Master enregistre une vente/recharge d'UV à un PDV (suite à son appel téléphonique)
// RÈGLE MÉTIER FERME : 1 UV vendu = 1 FCFA dû par le PDV. Le montant FCFA n'est jamais
// saisi séparément : il est TOUJOURS égal au montant en UV, pour que le fonds total
// (UV restant + FCFA encaissé) corresponde exactement, à tout moment, au total crédité par
// le superviseur — sans quoi le Master ne pourrait jamais détecter un manquant.
app.post('/master/recharges', requireRole('MASTER'), wrap(async (req, res) => {
  const pdvId = Number(req.body.pdv_id);
  const uv = numOrNaN(req.body.montant_uv);
  const fcfa = uv; // toujours 1:1 avec l'UV — jamais une valeur indépendante
  const paidNow = !!req.body.paid_now;
  if (!pdvId || !(uv > 0)) return res.status(400).json({ error: 'Le point de vente et le montant en UV sont obligatoires' });

  const pdv = await pool.query("SELECT id FROM users WHERE id=$1 AND role='PDV' AND actif=TRUE", [pdvId]);
  if (!pdv.rows.length) return res.status(404).json({ error: 'Point de vente introuvable' });

  const m = await pool.query('SELECT solde_uv FROM users WHERE id=$1', [req.user.id]);
  const soldeUv = Number(m.rows[0].solde_uv || 0);
  if (soldeUv < uv) return res.status(400).json({ error: 'Fonds UV insuffisant (solde disponible : ' + soldeUv + ')' });

  const statut = paidNow ? 'PAYE' : 'EN_ATTENTE';
  const r = await pool.query(
    `INSERT INTO uv_recharges (master_id, pdv_id, montant_uv, montant_fcfa, statut, paid_at)
     VALUES ($1,$2,$3,$4,$5,${paidNow ? 'now()' : 'NULL'}) RETURNING *`,
    [req.user.id, pdvId, uv, fcfa, statut]);

  const fund = paidNow
    ? await pool.query('UPDATE users SET solde_uv=solde_uv-$1, solde_fcfa=solde_fcfa+$2 WHERE id=$3 RETURNING solde_uv, solde_fcfa', [uv, fcfa, req.user.id])
    : await pool.query('UPDATE users SET solde_uv=solde_uv-$1 WHERE id=$2 RETURNING solde_uv, solde_fcfa', [uv, req.user.id]);

  broadcastToSupervisors({ type: 'recharge_created', masterId: req.user.id, recharge: r.rows[0] });
  res.json({ ok: true, recharge: r.rows[0], fund: fund.rows[0] });
}));

// Le Master encaisse le règlement d'une recharge restée en attente (paiement différé le même jour, ou plus tard)
app.post('/master/recharges/:id/pay', requireRole('MASTER'), wrap(async (req, res) => {
  const rc = await pool.query('SELECT * FROM uv_recharges WHERE id=$1 AND master_id=$2', [req.params.id, req.user.id]);
  if (!rc.rows.length) return res.status(404).json({ error: 'Recharge introuvable' });
  if (rc.rows[0].statut === 'PAYE') return res.json({ ok: true, already: true });
  await pool.query("UPDATE uv_recharges SET statut='PAYE', paid_at=now() WHERE id=$1", [req.params.id]);
  const fund = await pool.query('UPDATE users SET solde_fcfa=solde_fcfa+$1 WHERE id=$2 RETURNING solde_uv, solde_fcfa',
    [rc.rows[0].montant_fcfa, req.user.id]);
  broadcastToSupervisors({ type: 'recharge_paid', masterId: req.user.id, id: Number(req.params.id) });
  res.json({ ok: true, fund: fund.rows[0] });
}));

/* ---- Supervision des fonds Master (SUPERVISEUR uniquement) ---- */

// Le superviseur crédite manuellement le fonds (UV et/ou FCFA) d'un Master
app.post('/users/:id/credit', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const uv = numOrNaN(req.body.uv) || 0;
  const fcfa = numOrNaN(req.body.fcfa) || 0;
  if (!(uv > 0) && !(fcfa > 0)) return res.status(400).json({ error: 'Indique un montant en UV et/ou en FCFA' });
  const t = await pool.query('SELECT role FROM users WHERE id=$1', [req.params.id]);
  if (!t.rows.length) return res.status(404).json({ error: 'Compte introuvable' });
  if (t.rows[0].role !== 'MASTER') return res.status(400).json({ error: 'Seul un compte Master peut être crédité en UV/FCFA' });
  const r = await pool.query('UPDATE users SET solde_uv=solde_uv+$1, solde_fcfa=solde_fcfa+$2 WHERE id=$3 RETURNING solde_uv, solde_fcfa',
    [uv, fcfa, req.params.id]);
  await pool.query('INSERT INTO fund_credits (master_id, uv, fcfa) VALUES ($1,$2,$3)', [req.params.id, uv, fcfa]);
  sendToUser(req.params.id, { type: 'fund_credited', fund: r.rows[0], uv, fcfa });
  res.json({ ok: true, fund: r.rows[0] });
}));

// Le superviseur consulte l'historique des recharges d'un Master donné
app.get('/users/:id/recharges', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const r = await pool.query(
    `SELECT rc.*, u.nom_commercial, u.username AS pdv_username
     FROM uv_recharges rc JOIN users u ON u.id = rc.pdv_id
     WHERE rc.master_id=$1 ORDER BY rc.created_at DESC LIMIT 300`, [req.params.id]);
  res.json(r.rows);
}));

/* ---- COMPTE ÉQUIPE : gestion des comptes (SUPERVISEUR uniquement) ---- */
// Vue d'ensemble des fonds de tous les Masters (page dédiée superviseur)
// Statistiques globales (légères) — jamais besoin de charger tous les Masters pour ça
app.get('/masters/stats', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT COUNT(*) AS count,
      COALESCE(SUM(u.solde_uv), 0) AS total_uv,
      COALESCE(SUM(u.solde_fcfa), 0) AS total_fcfa,
      (SELECT COUNT(*) FROM uv_recharges rc JOIN users m ON m.id=rc.master_id WHERE m.role='MASTER' AND rc.statut='EN_ATTENTE') AS total_en_attente,
      (SELECT COUNT(*) FROM uv_recharges rc JOIN users m ON m.id=rc.master_id WHERE m.role='MASTER' AND rc.statut='EN_ATTENTE' AND rc.created_at < date_trunc('day', now())) AS total_en_retard
    FROM users u WHERE u.role='MASTER'`);
  const st = r.rows[0] || {};
  res.json({
    count: Number(st.count || 0),
    total_uv: Number(st.total_uv || 0),
    total_fcfa: Number(st.total_fcfa || 0),
    total_fonds: Number(st.total_uv || 0) + Number(st.total_fcfa || 0),
    total_en_attente: Number(st.total_en_attente || 0),
    total_en_retard: Number(st.total_en_retard || 0)
  });
}));

// Recherche de Masters par nom/prénom/identifiant — pagination légère (20 résultats max), jamais de liste complète
app.get('/masters/search', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const q = String(req.query.q || '').trim();
  let r;
  if (q) {
    r = await pool.query(
      `SELECT id, username, nom, prenoms FROM users
       WHERE role='MASTER' AND actif=TRUE AND (nom ILIKE $1 OR prenoms ILIKE $1 OR username ILIKE $1)
       ORDER BY nom NULLS LAST, prenoms NULLS LAST, username LIMIT 20`, ['%' + q + '%']);
  } else {
    r = await pool.query(
      `SELECT id, username, nom, prenoms FROM users WHERE role='MASTER' AND actif=TRUE
       ORDER BY nom NULLS LAST, prenoms NULLS LAST, username LIMIT 20`);
  }
  res.json(r.rows);
}));

// Fiche fonds détaillée d'UN Master (chargée uniquement quand le superviseur le sélectionne)
app.get('/masters/:id/summary', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT u.id, u.username, u.nom, u.prenoms, u.contact, u.solde_uv, u.solde_fcfa,
      COUNT(rc.id) FILTER (WHERE rc.statut='EN_ATTENTE') AS en_attente,
      COUNT(rc.id) FILTER (WHERE rc.statut='EN_ATTENTE' AND rc.created_at < date_trunc('day', now())) AS en_retard,
      COALESCE(SUM(rc.montant_uv) FILTER (WHERE rc.created_at >= date_trunc('day', now())), 0) AS uv_du_jour,
      MAX(rc.created_at) AS derniere_recharge,
      COALESCE((SELECT SUM(uv+fcfa) FROM fund_credits WHERE master_id=u.id), 0) AS total_credite
    FROM users u
    LEFT JOIN uv_recharges rc ON rc.master_id = u.id
    WHERE u.id=$1 AND u.role='MASTER'
    GROUP BY u.id`, [req.params.id]);
  if (!r.rows.length) return res.status(404).json({ error: 'Master introuvable' });
  const row = r.rows[0];
  ['solde_uv', 'solde_fcfa', 'total_credite', 'en_attente', 'en_retard', 'uv_du_jour'].forEach(k => { row[k] = Number(row[k] || 0); });
  row.fonds_total = row.solde_uv + row.solde_fcfa;
  row.en_circulation = row.total_credite - row.fonds_total;
  res.json(row);
}));

app.get('/users', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  // On exclut les images (photo, pièces) de la liste pour rester léger
  const baseCols = `id, username, role, nom, prenoms, contact, code, actif, created_at,
            last_lat AS lat, last_lng AS lng, loc_updated_at,
            nom_commercial, ville, quartier, zone, contact_responsable, contact_gerant,
            solde_uv, solde_fcfa,
            (photo IS NOT NULL) AS a_photo`;
  try {
    const r = await pool.query(
      `SELECT ${baseCols},
              (SELECT z.nom FROM zones z WHERE z.responsable_id = users.id LIMIT 1) AS zone_nom
       FROM users ORDER BY role, username`);
    res.json(r.rows);
  } catch (e) {
    // Repli si la table zones n'existe pas encore : la liste des comptes s'affiche quand même
    const r = await pool.query(`SELECT ${baseCols} FROM users ORDER BY role, username`);
    res.json(r.rows);
  }
}));

app.get('/users/:id', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const r = await pool.query('SELECT * FROM users WHERE id=$1', [req.params.id]);
  if (!r.rows.length) return res.status(404).json({ error: 'Compte introuvable' });
  const u = r.rows[0]; delete u.pass_hash;
  u.lat = u.last_lat; u.lng = u.last_lng; // alias attendus par le frontend
  if (u.role === 'COMMERCIAL') {
    try {
      const z = await pool.query(
        'SELECT id, nom, quartiers, (responsable_id=$1) AS est_chef FROM zones WHERE responsable_id=$1 OR suppleant_id=$1 ORDER BY (responsable_id=$1) DESC LIMIT 1', [u.id]);
      u.zone_info = z.rows[0] || null;
    } catch (e) { u.zone_info = null; } // ne bloque jamais la fiche si les zones ont un souci
  }
  res.json(u);
}));

const DEFAULT_PASSWORD = '0000'; // mot de passe par défaut à la création d'un compte

app.post('/users', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const b = req.body; const { username, role } = b;
  if (!username || !role)
    return res.status(400).json({ error: 'Identifiant et rôle sont obligatoires' });
  if (!['SUPERVISEUR', 'MASTER', 'COMMERCIAL', 'PDV'].includes(role))
    return res.status(400).json({ error: 'Rôle invalide' });
  const exists = await pool.query('SELECT 1 FROM users WHERE username=$1', [username]);
  if (exists.rows.length) return res.status(400).json({ error: 'Cet identifiant existe déjà' });
  const cols = ['username','pass_hash','role','must_change_password','nom','prenoms','contact','code','photo','piece_recto','piece_verso','nom_commercial','ville','quartier','zone','situation_geo','gps','nom_responsable','contact_responsable','nom_gerant','contact_gerant','photo_local'];
  const vals = [username, hashPass(DEFAULT_PASSWORD), role, true, b.nom||null, b.prenoms||null, b.contact||null, b.code||null, b.photo||null, b.piece_recto||null, b.piece_verso||null, b.nom_commercial||null, b.ville||null, b.quartier||null, b.zone||null, b.situation_geo||null, b.gps||null, b.nom_responsable||null, b.contact_responsable||null, b.nom_gerant||null, b.contact_gerant||null, b.photo_local||null];
  const ph = vals.map((_, i) => '$' + (i + 1)).join(',');
  const r = await pool.query(`INSERT INTO users (${cols.join(',')}) VALUES (${ph}) RETURNING id, username, role, nom, prenoms, contact, code, actif`, vals);
  // Si c'est un commercial rattaché à une zone, il en devient le responsable
  if (role === 'COMMERCIAL' && b.zone_id) {
    await pool.query('UPDATE zones SET responsable_id=$1 WHERE id=$2', [r.rows[0].id, b.zone_id]);
  }
  res.json(r.rows[0]);
}));

// Modifier les informations d'un agent (diffuse en direct)
app.put('/users/:id', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const b = req.body;
  await pool.query(
    `UPDATE users SET nom=$1, prenoms=$2, contact=$3, code=$4,
       nom_commercial=$5, ville=$6, quartier=$7, zone=$8, situation_geo=$9, gps=$10,
       nom_responsable=$11, contact_responsable=$12, nom_gerant=$13, contact_gerant=$14,
       photo=COALESCE($15,photo), piece_recto=COALESCE($16,piece_recto), piece_verso=COALESCE($17,piece_verso), photo_local=COALESCE($18,photo_local)
     WHERE id=$19`,
    [b.nom||null, b.prenoms||null, b.contact||null, b.code||null,
     b.nom_commercial||null, b.ville||null, b.quartier||null, b.zone||null, b.situation_geo||null, b.gps||null,
     b.nom_responsable||null, b.contact_responsable||null, b.nom_gerant||null, b.contact_gerant||null,
     b.photo||null, b.piece_recto||null, b.piece_verso||null, b.photo_local||null, req.params.id]);
  const r = await pool.query('SELECT id, username, role, nom, prenoms, contact, code, actif FROM users WHERE id=$1', [req.params.id]);
  if (r.rows.length) { const u = r.rows[0]; sendToUser(u.id, { type:'profile_updated', user:u }); broadcastToSupervisors({ type:'profile_updated', user:u }); }
  res.json({ ok: true });
}));

app.post('/users/:id/password', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  if (!req.body.password) return res.status(400).json({ error: 'Nouveau mot de passe requis' });
  // Réinitialisation = le compte devra redéfinir son mot de passe à sa prochaine connexion
  await pool.query('UPDATE users SET pass_hash=$1, failed_attempts=0, locked_until=NULL, must_change_password=TRUE WHERE id=$2',
    [hashPass(req.body.password), req.params.id]);
  await pool.query('DELETE FROM sessions WHERE user_id=$1', [req.params.id]);
  res.json({ ok: true });
}));

// Bloquer un compte : le rend inactif et coupe immédiatement sa session (il sera déconnecté à sa prochaine requête)
app.post('/users/:id/block', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const t = await pool.query('SELECT role, actif FROM users WHERE id=$1', [req.params.id]);
  if (!t.rows.length) return res.status(404).json({ error: 'Compte introuvable' });
  if (t.rows[0].role === 'SUPERVISEUR') return res.status(400).json({ error: 'Impossible de bloquer un superviseur' });
  if (t.rows[0].actif === false) return res.json({ ok: true, liveDisconnect: false }); // déjà bloqué
  await pool.query('UPDATE users SET actif=FALSE WHERE id=$1', [req.params.id]);
  await pool.query('DELETE FROM sessions WHERE user_id=$1', [req.params.id]);
  let live = false; wsClients.forEach(ws => { if (ws.userId === Number(req.params.id)) live = true; });
  kickUser(req.params.id);
  res.json({ ok: true, liveDisconnect: live });
}));

// Débloquer un compte : le rend de nouveau actif et lève un éventuel verrouillage anti-force brute
app.post('/users/:id/unblock', requireRole('SUPERVISEUR'), wrap(async (req, res) => {
  const t = await pool.query('SELECT id FROM users WHERE id=$1', [req.params.id]);
  if (!t.rows.length) return res.status(404).json({ error: 'Compte introuvable' });
  await pool.query('UPDATE users SET actif=TRUE, failed_attempts=0, locked_until=NULL WHERE id=$1', [req.params.id]);
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
const server = http.createServer(app);

// ---- Serveur WebSocket temps reel (meme port, chemin /ws) ----
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', async (ws, req) => {
  try {
    const u = new URL(req.url, 'http://localhost');
    const token = u.searchParams.get('token') || '';
    const r = await pool.query(
      `SELECT us.id, us.role FROM sessions s JOIN users us ON us.id = s.user_id
       WHERE s.token = $1 AND us.actif = TRUE`, [token]);
    if (!r.rows.length) { ws.close(); return; }
    ws.userId = r.rows[0].id; ws.role = r.rows[0].role;
    wsClients.add(ws);
    ws.on('close', () => wsClients.delete(ws));
    ws.on('error', () => { try { ws.close(); } catch (e) {} });
    // ping keepalive (evite les coupures des proxys)
    ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  } catch (e) { try { ws.close(); } catch (_) {} }
});
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) { try { ws.terminate(); } catch (e) {} return; }
    ws.isAlive = false; try { ws.ping(); } catch (e) {}
  });
}, 30000);

migrate()
  .then(() => server.listen(PORT, () => {
    console.log('\u2714 GAMServices demarre (HTTP + WebSocket) sur le port ' + PORT);
    checkOverdueRecharges();                          // vérif immédiate au démarrage
    setInterval(checkOverdueRecharges, 10 * 60 * 1000); // puis toutes les 10 minutes
  }))
  .catch(err => {
    console.error('\u2718 Echec migration au demarrage :', err.message);
    server.listen(PORT, () => console.log('\u26a0 Serveur demarre (migration en erreur) port ' + PORT));
  });
