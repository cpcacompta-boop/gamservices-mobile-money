/* =====================================================================
   GAMServices Holding — Backend Mobile Money
   Node.js / Express / PostgreSQL (Neon)
   - Migration automatique au démarrage (aucun SQL à lancer à la main)
   - Photos & pièces stockées en base64 (exclues des listes, servies au détail)
   - Logique +/- : sortie = négatif, entrée = positif (Grand livre)
   ===================================================================== */

const express = require('express');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());
// Limite élevée : on reçoit des images en base64
app.use(express.json({ limit: '15mb' }));
// Sert le frontend. On cherche index.html à côté de index.js OU dans un dossier "public".
// Comme ça ça marche quelle que soit la façon dont les fichiers sont rangés dans le dépôt.
const STATIC_DIR = [__dirname, path.join(__dirname, 'public')]
  .find(d => fs.existsSync(path.join(d, 'index.html'))) || __dirname;
app.use(express.static(STATIC_DIR));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Neon exige le SSL (valeur par défaut). Pour une base locale sans SSL : DB_SSL=off
  ssl: process.env.DB_SSL === 'off' ? false : { rejectUnauthorized: false }
});

/* =====================================================================
   1. MIGRATION AUTOMATIQUE
   Crée les tables si absentes, ajoute les colonnes manquantes.
   Fonctionne sur une base neuve ET sur ta base actuelle.
   ===================================================================== */
async function migrate() {
  const q = (s) => pool.query(s);

  // --- RESEAUX ---
  await q(`CREATE TABLE IF NOT EXISTS reseaux (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL,
    code TEXT,
    couleur TEXT,
    actif BOOLEAN DEFAULT TRUE
  )`);

  // --- MASTERS ---
  await q(`CREATE TABLE IF NOT EXISTS masters (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  await addCols('masters', {
    code: 'TEXT', prenoms: 'TEXT', contact: 'TEXT',
    photo: 'TEXT', piece_recto: 'TEXT', piece_verso: 'TEXT',
    adresse_siege: 'TEXT'
  });

  // --- SUPERVISEURS ---
  await q(`CREATE TABLE IF NOT EXISTS superviseurs (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  await addCols('superviseurs', {
    code: 'TEXT', prenoms: 'TEXT', contact: 'TEXT',
    photo: 'TEXT', piece_recto: 'TEXT', piece_verso: 'TEXT'
  });

  // --- COMMERCIAUX ---
  await q(`CREATE TABLE IF NOT EXISTS commerciaux (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  await addCols('commerciaux', {
    code: 'TEXT', prenoms: 'TEXT', contact: 'TEXT',
    photo: 'TEXT', permis_recto: 'TEXT', permis_verso: 'TEXT'
  });

  // --- ZONES ---
  await q(`CREATE TABLE IF NOT EXISTS zones (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL,
    commercial_id INTEGER REFERENCES commerciaux(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);

  // --- PDV ---
  await q(`CREATE TABLE IF NOT EXISTS pdv (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL,
    zone_id INTEGER REFERENCES zones(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  await addCols('pdv', {
    identifiant: 'TEXT', nom_commercial: 'TEXT',
    photo_responsable: 'TEXT', photo_local: 'TEXT',
    ville: 'TEXT', quartier: 'TEXT', situation_geo: 'TEXT',
    gps_lat: 'TEXT', gps_lng: 'TEXT',
    nom_responsable: 'TEXT', contact_responsable: 'TEXT',
    nom_gerant: 'TEXT', contact_gerant: 'TEXT',
    telephone: 'TEXT', est_proche_siege: 'BOOLEAN DEFAULT FALSE'
  });

  // --- OPERATIONS (Vente / Retour UV) ---
  await q(`CREATE TABLE IF NOT EXISTS operations (
    id SERIAL PRIMARY KEY,
    type_operation TEXT NOT NULL,
    reseau_id INTEGER REFERENCES reseaux(id),
    pdv_id INTEGER REFERENCES pdv(id) ON DELETE CASCADE,
    montant_du NUMERIC NOT NULL,
    statut TEXT DEFAULT 'EN_ATTENTE',
    initiateur_type TEXT,
    initiateur_id INTEGER,
    canal_deplacement TEXT DEFAULT 'DIRECT_SIEGE',
    date_operation DATE DEFAULT CURRENT_DATE,
    date_limite_reglement DATE DEFAULT (CURRENT_DATE + INTERVAL '2 days'),
    created_at TIMESTAMPTZ DEFAULT now()
  )`);

  // --- VERSEMENTS (multi-réseaux fractionnés) ---
  await q(`CREATE TABLE IF NOT EXISTS versements (
    id SERIAL PRIMARY KEY,
    operation_id INTEGER REFERENCES operations(id) ON DELETE CASCADE,
    mode TEXT NOT NULL,
    reseau_id INTEGER REFERENCES reseaux(id),
    montant NUMERIC NOT NULL,
    saisi_par_type TEXT,
    saisi_par_id INTEGER,
    note TEXT,
    date_versement TIMESTAMPTZ DEFAULT now()
  )`);

  // --- RAPPORT JOURNALIER ECART DE FONDS ---
  await q(`CREATE TABLE IF NOT EXISTS rapports_ecart (
    id SERIAL PRIMARY KEY,
    entite_type TEXT NOT NULL,     -- MASTER | COMMERCIAL | PDV
    entite_id INTEGER NOT NULL,
    date_rapport DATE DEFAULT CURRENT_DATE,
    solde_jour NUMERIC DEFAULT 0,
    solde_soir NUMERIC DEFAULT 0,
    type_ecart TEXT,               -- SURPLUS | MANQUANT | EQUILIBRE
    montant NUMERIC DEFAULT 0,
    motif TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);

  // --- TOURNEES (Point des opérations — commercial) ---
  await q(`CREATE TABLE IF NOT EXISTS tournees (
    id SERIAL PRIMARY KEY,
    commercial_id INTEGER REFERENCES commerciaux(id) ON DELETE CASCADE,
    date_tournee DATE DEFAULT CURRENT_DATE,
    montant_reverse NUMERIC DEFAULT 0,
    note TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  await q(`CREATE TABLE IF NOT EXISTS tournee_lignes (
    id SERIAL PRIMARY KEY,
    tournee_id INTEGER REFERENCES tournees(id) ON DELETE CASCADE,
    pdv_id INTEGER REFERENCES pdv(id) ON DELETE SET NULL,
    remise_retour NUMERIC DEFAULT 0,   -- Remise d'espèce ou Retour
    espece_achat NUMERIC DEFAULT 0,    -- Espèce reçu ou Achat UV
    observation TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);

  // --- POINTS DES OPERATIONS / AGENCE (PDV, réseau par réseau) ---
  await q(`CREATE TABLE IF NOT EXISTS agence_ops (
    id SERIAL PRIMARY KEY,
    pdv_id INTEGER REFERENCES pdv(id) ON DELETE CASCADE,
    date_op DATE DEFAULT CURRENT_DATE,
    periode TEXT,                       -- MATIN | SOIR
    espece_encaisse NUMERIC DEFAULT 0,
    transfert_unite NUMERIC DEFAULT 0,
    note TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);
  await q(`CREATE TABLE IF NOT EXISTS agence_ops_lignes (
    id SERIAL PRIMARY KEY,
    agence_op_id INTEGER REFERENCES agence_ops(id) ON DELETE CASCADE,
    reseau_id INTEGER REFERENCES reseaux(id),
    montant_uv NUMERIC DEFAULT 0
  )`);

  // --- MOUVEMENTS (Grand livre +/-) ---
  await q(`CREATE TABLE IF NOT EXISTS mouvements (
    id SERIAL PRIMARY KEY,
    acteur_type TEXT NOT NULL,          -- MASTER | COMMERCIAL | PDV | SUPERVISEUR
    acteur_id INTEGER NOT NULL,
    sens TEXT NOT NULL,                 -- ENTREE (+) | SORTIE (-)
    montant NUMERIC NOT NULL,
    motif TEXT,
    ref_type TEXT,
    ref_id INTEGER,
    date_mouvement DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);

  // --- APPROVISIONNEMENTS UV (chargement du stock chez un réseau) ---
  await q(`CREATE TABLE IF NOT EXISTS approvisionnements (
    id SERIAL PRIMARY KEY,
    reseau_id INTEGER REFERENCES reseaux(id),
    montant NUMERIC NOT NULL,
    date_appro DATE DEFAULT CURRENT_DATE,
    note TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
  )`);

  await seedReseaux();
  console.log('✔ Migration terminée : base à jour.');
}

// Ajoute des colonnes si elles n'existent pas encore
async function addCols(table, cols) {
  for (const [name, type] of Object.entries(cols)) {
    await pool.query(`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS ${name} ${type}`);
  }
}

// Réseaux Mobile Money (insérés une seule fois)
async function seedReseaux() {
  const list = [
    ['Wave', 'WAVE', '#1BA9E0'],
    ['Orange Money', 'OM', '#FF6A00'],
    ['MTN Money', 'MTN', '#F5B700'],
    ['Moov Money', 'MOOV', '#0072BC'],
    ['Trésor Money', 'TRESOR', '#2E7D5B'],
    ['Crédit Access', 'CREDIT_ACCESS', '#7A5CC0'],
    ['Cauridor', 'CAURIDOR', '#C0552E'],
    ['XPRESS Ecobank', 'XPRESS', '#0A5A3C'],
    ['Djamo', 'DJAMO', '#111827'],
    ['PUSH', 'PUSH', '#B5322A']
  ];
  for (const [nom, code, couleur] of list) {
    await pool.query(
      `INSERT INTO reseaux (nom, code, couleur)
       SELECT $1,$2,$3 WHERE NOT EXISTS (SELECT 1 FROM reseaux WHERE nom=$1)`,
      [nom, code, couleur]
    );
  }
}

/* =====================================================================
   2. HELPERS
   ===================================================================== */
// Enveloppe async → renvoie proprement les erreurs
const wrap = (fn) => (req, res) => fn(req, res).catch(err => {
  console.error(err);
  res.status(400).json({ error: err.message });
});

// Colonnes "lourdes" (images) à exclure des listes
const HEAVY = {
  masters: ['photo', 'piece_recto', 'piece_verso'],
  superviseurs: ['photo', 'piece_recto', 'piece_verso'],
  commerciaux: ['photo', 'permis_recto', 'permis_verso'],
  pdv: ['photo_responsable', 'photo_local']
};
// Construit la liste des colonnes sauf les lourdes (pour les GET liste)
function lightCols(table) {
  const heavy = HEAVY[table] || [];
  return `to_jsonb(t) - '${heavy.join("' - '")}'`;
}

/* =====================================================================
   3. SANTE + RESEAUX
   ===================================================================== */
app.get('/health', wrap(async (req, res) => {
  await pool.query('SELECT 1');
  res.json({ status: 'ok', database: 'connectée' });
}));

app.get('/reseaux', wrap(async (req, res) => {
  const r = await pool.query('SELECT * FROM reseaux WHERE actif = TRUE ORDER BY id');
  res.json(r.rows);
}));

/* =====================================================================
   4. CRUD GENERIQUE POUR LE PERSONNEL / PDV
   ===================================================================== */
function personnelRoutes(table, fields) {
  // LISTE (sans images)
  app.get(`/${table}`, wrap(async (req, res) => {
    const heavy = HEAVY[table] || [];
    const sel = heavy.length
      ? `(${lightCols(table)}) AS row`
      : `to_jsonb(t) AS row`;
    const r = await pool.query(`SELECT ${sel} FROM ${table} t ORDER BY t.created_at DESC`);
    res.json(r.rows.map(x => x.row));
  }));

  // DETAIL (avec images)
  app.get(`/${table}/:id`, wrap(async (req, res) => {
    const r = await pool.query(`SELECT * FROM ${table} WHERE id=$1`, [req.params.id]);
    if (!r.rows.length) return res.status(404).json({ error: 'Introuvable' });
    res.json(r.rows[0]);
  }));

  // CREATION
  app.post(`/${table}`, wrap(async (req, res) => {
    const cols = fields.filter(f => req.body[f] !== undefined);
    if (!cols.length) throw new Error('Aucune donnée fournie');
    const vals = cols.map(c => normalize(req.body[c]));
    const ph = cols.map((_, i) => `$${i + 1}`);
    const r = await pool.query(
      `INSERT INTO ${table} (${cols.join(',')}) VALUES (${ph.join(',')}) RETURNING id`,
      vals
    );
    res.json({ id: r.rows[0].id, ok: true });
  }));

  // MODIFICATION
  app.put(`/${table}/:id`, wrap(async (req, res) => {
    const cols = fields.filter(f => req.body[f] !== undefined);
    if (!cols.length) throw new Error('Aucune donnée à modifier');
    const set = cols.map((c, i) => `${c}=$${i + 1}`);
    const vals = cols.map(c => normalize(req.body[c]));
    vals.push(req.params.id);
    await pool.query(`UPDATE ${table} SET ${set.join(',')} WHERE id=$${vals.length}`, vals);
    res.json({ ok: true });
  }));

  // SUPPRESSION
  app.delete(`/${table}/:id`, wrap(async (req, res) => {
    await pool.query(`DELETE FROM ${table} WHERE id=$1`, [req.params.id]);
    res.json({ ok: true });
  }));
}
// Nettoie les chaînes vides → null
function normalize(v) {
  if (v === '' || v === undefined) return null;
  return v;
}

personnelRoutes('masters', ['code', 'nom', 'prenoms', 'contact', 'photo', 'piece_recto', 'piece_verso', 'adresse_siege']);
personnelRoutes('superviseurs', ['code', 'nom', 'prenoms', 'contact', 'photo', 'piece_recto', 'piece_verso']);
personnelRoutes('commerciaux', ['code', 'nom', 'prenoms', 'contact', 'photo', 'permis_recto', 'permis_verso']);

/* =====================================================================
   5. ZONES
   ===================================================================== */
app.get('/zones', wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT z.*, c.nom AS commercial_nom, c.prenoms AS commercial_prenoms
    FROM zones z
    LEFT JOIN commerciaux c ON c.id = z.commercial_id
    ORDER BY z.created_at DESC
  `);
  res.json(r.rows);
}));
app.post('/zones', wrap(async (req, res) => {
  const { nom, commercial_id } = req.body;
  const r = await pool.query(
    'INSERT INTO zones (nom, commercial_id) VALUES ($1,$2) RETURNING id',
    [nom, commercial_id || null]
  );
  res.json({ id: r.rows[0].id, ok: true });
}));
app.put('/zones/:id', wrap(async (req, res) => {
  const { nom, commercial_id } = req.body;
  await pool.query('UPDATE zones SET nom=$1, commercial_id=$2 WHERE id=$3',
    [nom, commercial_id || null, req.params.id]);
  res.json({ ok: true });
}));
app.delete('/zones/:id', wrap(async (req, res) => {
  await pool.query('DELETE FROM zones WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
}));

/* =====================================================================
   6. PDV
   ===================================================================== */
const PDV_FIELDS = ['identifiant', 'nom_commercial', 'zone_id', 'ville', 'quartier',
  'situation_geo', 'gps_lat', 'gps_lng', 'nom_responsable', 'contact_responsable',
  'nom_gerant', 'contact_gerant', 'photo_responsable', 'photo_local',
  'est_proche_siege', 'telephone'];

app.get('/pdv', wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT (${lightCols('pdv')}) || jsonb_build_object('zone_nom', z.nom) AS row
    FROM pdv t
    LEFT JOIN zones z ON z.id = t.zone_id
    ORDER BY t.created_at DESC
  `);
  res.json(r.rows.map(x => x.row));
}));

app.get('/pdv/:id', wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT p.*, z.nom AS zone_nom FROM pdv p
    LEFT JOIN zones z ON z.id = p.zone_id WHERE p.id=$1`, [req.params.id]);
  if (!r.rows.length) return res.status(404).json({ error: 'Introuvable' });
  res.json(r.rows[0]);
}));

app.post('/pdv', wrap(async (req, res) => {
  const b = req.body;
  const cols = PDV_FIELDS.filter(f => b[f] !== undefined);
  // "nom" reste rempli = nom_commercial pour compatibilité (jointures opérations)
  cols.push('nom');
  const vals = cols.map(c => {
    if (c === 'nom') return normalize(b.nom_commercial || b.identifiant || 'PDV');
    if (c === 'est_proche_siege') return b[c] === true || b[c] === 'true';
    return normalize(b[c]);
  });
  const ph = cols.map((_, i) => `$${i + 1}`);
  const r = await pool.query(
    `INSERT INTO pdv (${cols.join(',')}) VALUES (${ph.join(',')}) RETURNING id`, vals);
  res.json({ id: r.rows[0].id, ok: true });
}));

app.put('/pdv/:id', wrap(async (req, res) => {
  const b = req.body;
  const cols = PDV_FIELDS.filter(f => b[f] !== undefined);
  if (b.nom_commercial !== undefined) cols.push('nom');
  const set = cols.map((c, i) => `${c}=$${i + 1}`);
  const vals = cols.map(c => {
    if (c === 'nom') return normalize(b.nom_commercial);
    if (c === 'est_proche_siege') return b[c] === true || b[c] === 'true';
    return normalize(b[c]);
  });
  vals.push(req.params.id);
  await pool.query(`UPDATE pdv SET ${set.join(',')} WHERE id=$${vals.length}`, vals);
  res.json({ ok: true });
}));

app.delete('/pdv/:id', wrap(async (req, res) => {
  await pool.query('DELETE FROM pdv WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
}));

/* =====================================================================
   7. OPERATIONS + VERSEMENTS
   ===================================================================== */
app.post('/operations', wrap(async (req, res) => {
  const { type_operation, reseau_id, pdv_id, montant_du,
    initiateur_type, initiateur_id, canal_deplacement, date_limite_reglement } = req.body;
  const r = await pool.query(
    `INSERT INTO operations
       (type_operation, reseau_id, pdv_id, montant_du, initiateur_type, initiateur_id, canal_deplacement, date_limite_reglement)
     VALUES ($1,$2,$3,$4,$5,$6,$7, COALESCE($8, CURRENT_DATE + INTERVAL '2 days'))
     RETURNING id`,
    [type_operation, reseau_id, pdv_id, montant_du, initiateur_type || null,
     initiateur_id || null, canal_deplacement || 'DIRECT_SIEGE', date_limite_reglement || null]
  );
  res.json({ id: r.rows[0].id, ok: true });
}));

const OPS_SELECT = `
  SELECT
    o.id, o.type_operation, o.montant_du, o.statut, o.date_operation, o.date_limite_reglement,
    o.initiateur_type, o.initiateur_id, o.canal_deplacement,
    p.id AS pdv_id, p.nom AS pdv_nom,
    r.id AS reseau_id, r.nom AS reseau_nom, r.couleur AS reseau_couleur,
    COALESCE(SUM(v.montant), 0) AS montant_verse,
    o.montant_du - COALESCE(SUM(v.montant), 0) AS montant_restant
  FROM operations o
  JOIN pdv p ON p.id = o.pdv_id
  LEFT JOIN reseaux r ON r.id = o.reseau_id
  LEFT JOIN versements v ON v.operation_id = o.id
  GROUP BY o.id, p.id, p.nom, r.id, r.nom, r.couleur
`;

app.get('/operations', wrap(async (req, res) => {
  const r = await pool.query(OPS_SELECT + ' ORDER BY o.date_operation DESC, o.created_at DESC');
  res.json(r.rows.map(deriveStatut));
}));

function deriveStatut(o) {
  const reste = Number(o.montant_restant);
  const verse = Number(o.montant_verse);
  if (reste <= 0) o.statut = 'SOLDE';
  else if (verse > 0) o.statut = 'PARTIEL';
  else o.statut = 'EN_ATTENTE';
  return o;
}

app.post('/versements', wrap(async (req, res) => {
  const { operation_id, mode, reseau_id, montant, saisi_par_type, saisi_par_id, note } = req.body;
  await pool.query(
    `INSERT INTO versements (operation_id, mode, reseau_id, montant, saisi_par_type, saisi_par_id, note)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [operation_id, mode, mode === 'UV' ? reseau_id : null, montant,
     saisi_par_type || null, saisi_par_id || null, note || null]
  );
  // Met à jour le statut de l'opération
  await pool.query(`
    UPDATE operations o SET statut = CASE
      WHEN o.montant_du - COALESCE((SELECT SUM(montant) FROM versements WHERE operation_id=o.id),0) <= 0 THEN 'SOLDE'
      WHEN COALESCE((SELECT SUM(montant) FROM versements WHERE operation_id=o.id),0) > 0 THEN 'PARTIEL'
      ELSE 'EN_ATTENTE' END
    WHERE o.id = $1`, [operation_id]);
  res.json({ ok: true });
}));

app.get('/versements/:operation_id', wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT v.*, r.nom AS reseau_nom, r.couleur AS reseau_couleur
    FROM versements v
    LEFT JOIN reseaux r ON r.id = v.reseau_id
    WHERE v.operation_id = $1 ORDER BY v.date_versement DESC`, [req.params.operation_id]);
  res.json(r.rows);
}));

/* =====================================================================
   8. ALERTES (PDV n'ayant pas soldé)
   ===================================================================== */
app.get('/alertes', wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT o.id AS operation_id, o.type_operation, o.date_limite_reglement, o.date_operation,
      p.nom AS pdv_nom, r.nom AS reseau_nom, r.couleur AS reseau_couleur,
      o.montant_du,
      COALESCE(SUM(v.montant), 0) AS montant_verse,
      o.montant_du - COALESCE(SUM(v.montant), 0) AS montant_restant,
      (o.date_limite_reglement < CURRENT_DATE) AS en_retard
    FROM operations o
    JOIN pdv p ON p.id = o.pdv_id
    LEFT JOIN reseaux r ON r.id = o.reseau_id
    LEFT JOIN versements v ON v.operation_id = o.id
    GROUP BY o.id, p.nom, r.nom, r.couleur
    HAVING o.montant_du - COALESCE(SUM(v.montant), 0) > 0
    ORDER BY o.date_limite_reglement ASC
  `);
  res.json(r.rows);
}));

/* =====================================================================
   9. RAPPORT JOURNALIER — ECART DE FONDS
   ===================================================================== */
app.get('/rapports-ecart', wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT re.*,
      CASE re.entite_type
        WHEN 'MASTER' THEN (SELECT nom FROM masters WHERE id=re.entite_id)
        WHEN 'COMMERCIAL' THEN (SELECT nom FROM commerciaux WHERE id=re.entite_id)
        WHEN 'PDV' THEN (SELECT nom FROM pdv WHERE id=re.entite_id)
      END AS entite_nom
    FROM rapports_ecart re
    ORDER BY re.date_rapport DESC, re.created_at DESC
  `);
  res.json(r.rows);
}));
app.post('/rapports-ecart', wrap(async (req, res) => {
  const { entite_type, entite_id, date_rapport, solde_jour, solde_soir, motif } = req.body;
  const jour = Number(solde_jour) || 0;
  const soir = Number(solde_soir) || 0;
  const diff = soir - jour;
  const type_ecart = diff > 0 ? 'SURPLUS' : diff < 0 ? 'MANQUANT' : 'EQUILIBRE';
  await pool.query(
    `INSERT INTO rapports_ecart (entite_type, entite_id, date_rapport, solde_jour, solde_soir, type_ecart, montant, motif)
     VALUES ($1,$2,COALESCE($3,CURRENT_DATE),$4,$5,$6,$7,$8)`,
    [entite_type, entite_id, date_rapport || null, jour, soir, type_ecart, Math.abs(diff), motif || null]
  );
  res.json({ ok: true });
}));
app.delete('/rapports-ecart/:id', wrap(async (req, res) => {
  await pool.query('DELETE FROM rapports_ecart WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
}));

/* =====================================================================
   10. TOURNEES — Point des opérations (commercial)
   ===================================================================== */
app.get('/tournees', wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT t.*, c.nom AS commercial_nom, c.prenoms AS commercial_prenoms,
      COALESCE(SUM(l.remise_retour + l.espece_achat),0) AS total_collecte,
      COALESCE(SUM(l.remise_retour + l.espece_achat),0) - t.montant_reverse AS reste_a_reverser,
      COUNT(l.id) AS nb_lignes
    FROM tournees t
    LEFT JOIN commerciaux c ON c.id = t.commercial_id
    LEFT JOIN tournee_lignes l ON l.tournee_id = t.id
    GROUP BY t.id, c.nom, c.prenoms
    ORDER BY t.date_tournee DESC, t.created_at DESC
  `);
  res.json(r.rows);
}));
app.get('/tournees/:id', wrap(async (req, res) => {
  const t = await pool.query(`
    SELECT t.*, c.nom AS commercial_nom, c.prenoms AS commercial_prenoms
    FROM tournees t LEFT JOIN commerciaux c ON c.id=t.commercial_id WHERE t.id=$1`, [req.params.id]);
  if (!t.rows.length) return res.status(404).json({ error: 'Introuvable' });
  const l = await pool.query(`
    SELECT l.*, p.nom AS pdv_nom FROM tournee_lignes l
    LEFT JOIN pdv p ON p.id=l.pdv_id WHERE l.tournee_id=$1 ORDER BY l.id`, [req.params.id]);
  res.json({ ...t.rows[0], lignes: l.rows });
}));
app.post('/tournees', wrap(async (req, res) => {
  const { commercial_id, date_tournee, montant_reverse, note, lignes } = req.body;
  const t = await pool.query(
    `INSERT INTO tournees (commercial_id, date_tournee, montant_reverse, note)
     VALUES ($1, COALESCE($2,CURRENT_DATE), $3, $4) RETURNING id`,
    [commercial_id, date_tournee || null, Number(montant_reverse) || 0, note || null]);
  const id = t.rows[0].id;
  if (Array.isArray(lignes)) {
    for (const li of lignes) {
      await pool.query(
        `INSERT INTO tournee_lignes (tournee_id, pdv_id, remise_retour, espece_achat, observation)
         VALUES ($1,$2,$3,$4,$5)`,
        [id, li.pdv_id || null, Number(li.remise_retour) || 0, Number(li.espece_achat) || 0, li.observation || null]);
    }
  }
  res.json({ id, ok: true });
}));
app.delete('/tournees/:id', wrap(async (req, res) => {
  await pool.query('DELETE FROM tournees WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
}));

/* =====================================================================
   11. POINTS DES OPERATIONS / AGENCE (PDV, réseau par réseau)
   ===================================================================== */
app.get('/agence-ops', wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT a.*, p.nom AS pdv_nom,
      COALESCE((SELECT SUM(montant_uv) FROM agence_ops_lignes WHERE agence_op_id=a.id),0) AS total_uv,
      COALESCE((SELECT SUM(montant_uv) FROM agence_ops_lignes WHERE agence_op_id=a.id),0)
        + a.espece_encaisse AS montant_total
    FROM agence_ops a
    LEFT JOIN pdv p ON p.id=a.pdv_id
    ORDER BY a.date_op DESC, a.created_at DESC
  `);
  res.json(r.rows);
}));
app.get('/agence-ops/:id', wrap(async (req, res) => {
  const a = await pool.query(`
    SELECT a.*, p.nom AS pdv_nom FROM agence_ops a
    LEFT JOIN pdv p ON p.id=a.pdv_id WHERE a.id=$1`, [req.params.id]);
  if (!a.rows.length) return res.status(404).json({ error: 'Introuvable' });
  const l = await pool.query(`
    SELECT l.*, r.nom AS reseau_nom, r.couleur AS reseau_couleur
    FROM agence_ops_lignes l LEFT JOIN reseaux r ON r.id=l.reseau_id
    WHERE l.agence_op_id=$1 ORDER BY r.id`, [req.params.id]);
  res.json({ ...a.rows[0], lignes: l.rows });
}));
app.post('/agence-ops', wrap(async (req, res) => {
  const { pdv_id, date_op, periode, espece_encaisse, transfert_unite, note, lignes } = req.body;
  const a = await pool.query(
    `INSERT INTO agence_ops (pdv_id, date_op, periode, espece_encaisse, transfert_unite, note)
     VALUES ($1, COALESCE($2,CURRENT_DATE), $3, $4, $5, $6) RETURNING id`,
    [pdv_id, date_op || null, periode || null, Number(espece_encaisse) || 0,
     Number(transfert_unite) || 0, note || null]);
  const id = a.rows[0].id;
  if (Array.isArray(lignes)) {
    for (const li of lignes) {
      if (li.reseau_id && Number(li.montant_uv) > 0) {
        await pool.query(
          `INSERT INTO agence_ops_lignes (agence_op_id, reseau_id, montant_uv) VALUES ($1,$2,$3)`,
          [id, li.reseau_id, Number(li.montant_uv) || 0]);
      }
    }
  }
  res.json({ id, ok: true });
}));
app.delete('/agence-ops/:id', wrap(async (req, res) => {
  await pool.query('DELETE FROM agence_ops WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
}));

/* =====================================================================
   12. GRAND LIVRE — MOUVEMENTS (+/-)
   sortie = SORTIE (-), entrée = ENTREE (+)
   ===================================================================== */
app.get('/mouvements', wrap(async (req, res) => {
  const { acteur_type, acteur_id } = req.query;
  let sql = `
    SELECT m.*,
      CASE m.acteur_type
        WHEN 'MASTER' THEN (SELECT nom FROM masters WHERE id=m.acteur_id)
        WHEN 'COMMERCIAL' THEN (SELECT nom FROM commerciaux WHERE id=m.acteur_id)
        WHEN 'SUPERVISEUR' THEN (SELECT nom FROM superviseurs WHERE id=m.acteur_id)
        WHEN 'PDV' THEN (SELECT nom FROM pdv WHERE id=m.acteur_id)
      END AS acteur_nom,
      CASE WHEN m.sens='ENTREE' THEN m.montant ELSE -m.montant END AS montant_signe
    FROM mouvements m`;
  const params = [];
  const conds = [];
  if (acteur_type) { params.push(acteur_type); conds.push(`m.acteur_type=$${params.length}`); }
  if (acteur_id) { params.push(acteur_id); conds.push(`m.acteur_id=$${params.length}`); }
  if (conds.length) sql += ' WHERE ' + conds.join(' AND ');
  sql += ' ORDER BY m.date_mouvement DESC, m.created_at DESC';
  const r = await pool.query(sql, params);
  res.json(r.rows);
}));

// Soldes par acteur (+/-)
app.get('/soldes', wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT m.acteur_type, m.acteur_id,
      CASE m.acteur_type
        WHEN 'MASTER' THEN (SELECT nom FROM masters WHERE id=m.acteur_id)
        WHEN 'COMMERCIAL' THEN (SELECT nom FROM commerciaux WHERE id=m.acteur_id)
        WHEN 'SUPERVISEUR' THEN (SELECT nom FROM superviseurs WHERE id=m.acteur_id)
        WHEN 'PDV' THEN (SELECT nom FROM pdv WHERE id=m.acteur_id)
      END AS acteur_nom,
      SUM(CASE WHEN m.sens='ENTREE' THEN m.montant ELSE -m.montant END) AS solde
    FROM mouvements m
    GROUP BY m.acteur_type, m.acteur_id
    ORDER BY solde ASC
  `);
  res.json(r.rows);
}));

app.post('/mouvements', wrap(async (req, res) => {
  const { acteur_type, acteur_id, sens, montant, motif, date_mouvement } = req.body;
  await pool.query(
    `INSERT INTO mouvements (acteur_type, acteur_id, sens, montant, motif, date_mouvement)
     VALUES ($1,$2,$3,$4,$5,COALESCE($6,CURRENT_DATE))`,
    [acteur_type, acteur_id, sens, Math.abs(Number(montant)) || 0, motif || null, date_mouvement || null]);
  res.json({ ok: true });
}));
app.delete('/mouvements/:id', wrap(async (req, res) => {
  await pool.query('DELETE FROM mouvements WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
}));

/* =====================================================================
   13. STATS (tableau de bord)
   ===================================================================== */
app.get('/stats', wrap(async (req, res) => {
  const [pdv, ops, alertes, du] = await Promise.all([
    pool.query('SELECT COUNT(*) FROM pdv'),
    pool.query('SELECT COUNT(*) FROM operations'),
    pool.query(`SELECT COUNT(*) FROM (
      SELECT o.id FROM operations o LEFT JOIN versements v ON v.operation_id=o.id
      GROUP BY o.id HAVING o.montant_du - COALESCE(SUM(v.montant),0) > 0) x`),
    pool.query(`SELECT COALESCE(SUM(montant_du),0) AS total FROM operations`)
  ]);
  res.json({
    pdv: Number(pdv.rows[0].count),
    operations: Number(ops.rows[0].count),
    alertes: Number(alertes.rows[0].count),
    total_du: Number(du.rows[0].total)
  });
}));

/* =====================================================================
   APPROVISIONNEMENTS UV
   ===================================================================== */
app.post('/approvisionnements', wrap(async (req, res) => {
  const { reseau_id, montant, date_appro, note } = req.body;
  const r = await pool.query(
    `INSERT INTO approvisionnements (reseau_id, montant, date_appro, note)
     VALUES ($1,$2,$3,$4) RETURNING *`,
    [reseau_id, montant, date_appro || null, note || null]);
  res.json(r.rows[0]);
}));

app.get('/approvisionnements', wrap(async (req, res) => {
  const r = await pool.query(`
    SELECT a.*, rz.nom AS reseau_nom, rz.couleur AS reseau_couleur
    FROM approvisionnements a
    LEFT JOIN reseaux rz ON rz.id = a.reseau_id
    ORDER BY a.date_appro DESC, a.created_at DESC`);
  res.json(r.rows);
}));

app.delete('/approvisionnements/:id', wrap(async (req, res) => {
  await pool.query('DELETE FROM approvisionnements WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
}));

/* =====================================================================
   COCKPIT : pilotage global
   (trésorerie, stock UV par réseau, créances, commerciaux)
   ===================================================================== */
app.get('/cockpit', wrap(async (req, res) => {
  const [glob, reseaux, commerciaux, debiteurs] = await Promise.all([
    pool.query(`
      SELECT
        (SELECT COALESCE(SUM(montant),0) FROM approvisionnements) AS total_appro,
        (SELECT COALESCE(SUM(montant_du),0) FROM operations WHERE type_operation='VENTE_UV') AS total_vendu,
        (SELECT COALESCE(SUM(montant_du),0) FROM operations WHERE type_operation='RETOUR_UV') AS total_retour,
        (SELECT COALESCE(SUM(montant),0) FROM versements) AS total_encaisse,
        (SELECT COALESCE(SUM(GREATEST(o.montant_du - COALESCE((SELECT SUM(montant) FROM versements v WHERE v.operation_id=o.id),0),0)),0) FROM operations o) AS total_creances,
        (SELECT COUNT(*) FROM (
            SELECT o.id FROM operations o
            WHERE o.montant_du - COALESCE((SELECT SUM(montant) FROM versements v WHERE v.operation_id=o.id),0) > 0
        ) z) AS nb_impayes
    `),
    pool.query(`
      SELECT rz.id, rz.nom, rz.couleur,
        COALESCE(ap.appro,0)   AS approvisionne,
        COALESCE(op.vendu,0)   AS vendu,
        COALESCE(op.retour,0)  AS retour,
        COALESCE(ap.appro,0) - COALESCE(op.vendu,0) + COALESCE(op.retour,0) AS stock,
        COALESCE(vs.encaisse_uv,0) AS encaisse_uv,
        COALESCE(cr.creances,0)    AS creances
      FROM reseaux rz
      LEFT JOIN (SELECT reseau_id, SUM(montant) AS appro FROM approvisionnements GROUP BY reseau_id) ap ON ap.reseau_id=rz.id
      LEFT JOIN (SELECT reseau_id,
          SUM(CASE WHEN type_operation='VENTE_UV'  THEN montant_du ELSE 0 END) AS vendu,
          SUM(CASE WHEN type_operation='RETOUR_UV' THEN montant_du ELSE 0 END) AS retour
          FROM operations GROUP BY reseau_id) op ON op.reseau_id=rz.id
      LEFT JOIN (SELECT reseau_id, SUM(montant) AS encaisse_uv FROM versements WHERE mode='UV' GROUP BY reseau_id) vs ON vs.reseau_id=rz.id
      LEFT JOIN (SELECT o.reseau_id, SUM(GREATEST(o.montant_du - COALESCE((SELECT SUM(montant) FROM versements v WHERE v.operation_id=o.id),0),0)) AS creances FROM operations o GROUP BY o.reseau_id) cr ON cr.reseau_id=rz.id
      ORDER BY rz.nom
    `),
    pool.query(`
      SELECT c.id, c.nom, c.prenoms,
        COALESCE(pz.nb_pdv,0)      AS nb_pdv,
        COALESCE(tv.nb_tournees,0) AS nb_tournees,
        COALESCE(tc.collecte,0)    AS collecte,
        COALESCE(tv.reverse,0)     AS reverse,
        COALESCE(tc.collecte,0) - COALESCE(tv.reverse,0) AS reste_a_reverser
      FROM commerciaux c
      LEFT JOIN (SELECT z.commercial_id, COUNT(p.id) AS nb_pdv FROM zones z LEFT JOIN pdv p ON p.zone_id=z.id GROUP BY z.commercial_id) pz ON pz.commercial_id=c.id
      LEFT JOIN (SELECT commercial_id, COUNT(*) AS nb_tournees, SUM(montant_reverse) AS reverse FROM tournees GROUP BY commercial_id) tv ON tv.commercial_id=c.id
      LEFT JOIN (SELECT t.commercial_id, SUM(l.remise_retour + l.espece_achat) AS collecte FROM tournees t JOIN tournee_lignes l ON l.tournee_id=t.id GROUP BY t.commercial_id) tc ON tc.commercial_id=c.id
      ORDER BY reste_a_reverser DESC, c.nom
    `),
    pool.query(`
      SELECT p.id, p.nom AS pdv_nom, z.nom AS zone_nom,
        SUM(GREATEST(o.montant_du - COALESCE((SELECT SUM(montant) FROM versements v WHERE v.operation_id=o.id),0),0)) AS dette
      FROM operations o
      JOIN pdv p ON p.id=o.pdv_id
      LEFT JOIN zones z ON z.id=p.zone_id
      GROUP BY p.id, p.nom, z.nom
      HAVING SUM(GREATEST(o.montant_du - COALESCE((SELECT SUM(montant) FROM versements v WHERE v.operation_id=o.id),0),0)) > 0
      ORDER BY dette DESC
      LIMIT 10
    `)
  ]);
  res.json({
    global: glob.rows[0],
    reseaux: reseaux.rows,
    commerciaux: commerciaux.rows,
    debiteurs: debiteurs.rows
  });
}));

/* =====================================================================
   RECOUVREMENT : factures impayées par zone / commercial (terrain)
   ===================================================================== */
app.get('/recouvrement', wrap(async (req, res) => {
  const rows = await pool.query(`
    SELECT o.id, o.type_operation, o.montant_du, o.date_operation, o.date_limite_reglement,
      o.montant_du - COALESCE((SELECT SUM(montant) FROM versements v WHERE v.operation_id=o.id),0) AS montant_restant,
      p.id AS pdv_id, p.nom AS pdv_nom, p.telephone AS pdv_tel,
      z.id AS zone_id, z.nom AS zone_nom,
      c.id AS commercial_id, c.nom AS commercial_nom, c.prenoms AS commercial_prenoms,
      rz.nom AS reseau_nom, rz.couleur AS reseau_couleur
    FROM operations o
    JOIN pdv p ON p.id = o.pdv_id
    LEFT JOIN zones z ON z.id = p.zone_id
    LEFT JOIN commerciaux c ON c.id = z.commercial_id
    LEFT JOIN reseaux rz ON rz.id = o.reseau_id
    WHERE o.montant_du - COALESCE((SELECT SUM(montant) FROM versements v WHERE v.operation_id=o.id),0) > 0
    ORDER BY z.nom NULLS LAST, p.nom, o.date_operation
  `);
  res.json(rows.rows);
}));

/* =====================================================================
   FILET DE SÉCURITÉ : toute page non-API renvoie index.html
   (évite définitivement le message "Cannot GET /")
   ===================================================================== */
app.get('*', (req, res) => {
  const file = path.join(STATIC_DIR, 'index.html');
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(200).type('html').send(
    '<div style="font-family:sans-serif;max-width:560px;margin:60px auto;line-height:1.6">' +
    '<h2>Backend GAMServices en ligne ✅</h2>' +
    '<p>Le serveur tourne bien, mais le fichier <b>index.html</b> est introuvable.</p>' +
    '<p>Vérifie que <b>index.html</b> se trouve dans le <b>même dossier</b> que <b>index.js</b> ' +
    'dans ton dépôt GitHub, puis relance le déploiement.</p></div>'
  );
});

/* =====================================================================
   14. DEMARRAGE
   ===================================================================== */
const PORT = process.env.PORT || 3000;
migrate()
  .then(() => {
    app.listen(PORT, () => console.log(`✔ GAMServices démarré sur le port ${PORT}`));
  })
  .catch(err => {
    console.error('✘ Échec migration au démarrage :', err.message);
    // On démarre quand même le serveur pour /health
    app.listen(PORT, () => console.log(`⚠ Serveur démarré (migration en erreur) port ${PORT}`));
  });
