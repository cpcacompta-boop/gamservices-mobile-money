const express = require('express');
const path = require('path');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

// Sert les fichiers du frontend (dossier "public")
app.use(express.static(path.join(__dirname, 'public')));

// Connexion à la base Neon
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// ---- SANTE ----
app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'ok', database: 'connectée' });
  } catch (err) {
    res.status(500).json({ status: 'erreur', message: err.message });
  }
});

// ---- RESEAUX ----
app.get('/reseaux', async (req, res) => {
  const result = await pool.query('SELECT * FROM reseaux ORDER BY nom');
  res.json(result.rows);
});

// ---- MASTERS ----
app.post('/masters', async (req, res) => {
  try {
    const { nom, telephone, adresse_siege } = req.body;
    const result = await pool.query(
      'INSERT INTO masters (nom, telephone, adresse_siege) VALUES ($1,$2,$3) RETURNING *',
      [nom, telephone, adresse_siege]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/masters', async (req, res) => {
  const result = await pool.query('SELECT * FROM masters ORDER BY created_at DESC');
  res.json(result.rows);
});

// ---- COMMERCIAUX ----
app.post('/commerciaux', async (req, res) => {
  try {
    const { nom, telephone } = req.body;
    const result = await pool.query(
      'INSERT INTO commerciaux (nom, telephone) VALUES ($1,$2) RETURNING *',
      [nom, telephone]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/commerciaux', async (req, res) => {
  const result = await pool.query('SELECT * FROM commerciaux ORDER BY created_at DESC');
  res.json(result.rows);
});

// ---- ZONES ----
app.post('/zones', async (req, res) => {
  try {
    const { nom, commercial_id } = req.body;
    const result = await pool.query(
      'INSERT INTO zones (nom, commercial_id) VALUES ($1,$2) RETURNING *',
      [nom, commercial_id]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/zones', async (req, res) => {
  const result = await pool.query(`
    SELECT z.*, c.nom AS commercial_nom
    FROM zones z
    JOIN commerciaux c ON c.id = z.commercial_id
    ORDER BY z.created_at DESC
  `);
  res.json(result.rows);
});

// ---- PDV ----
app.post('/pdv', async (req, res) => {
  try {
    const { nom, telephone, zone_id, est_proche_siege } = req.body;
    const result = await pool.query(
      'INSERT INTO pdv (nom, telephone, zone_id, est_proche_siege) VALUES ($1,$2,$3,$4) RETURNING *',
      [nom, telephone, zone_id, est_proche_siege === true || est_proche_siege === 'true']
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/pdv', async (req, res) => {
  const result = await pool.query(`
    SELECT p.*, z.nom AS zone_nom
    FROM pdv p
    JOIN zones z ON z.id = p.zone_id
    ORDER BY p.created_at DESC
  `);
  res.json(result.rows);
});

// ---- OPERATIONS ----
app.post('/operations', async (req, res) => {
  try {
    const { type_operation, reseau_id, pdv_id, montant_du, initiateur_type, initiateur_id, canal_deplacement } = req.body;
    const result = await pool.query(
      `INSERT INTO operations (type_operation, reseau_id, pdv_id, montant_du, initiateur_type, initiateur_id, canal_deplacement)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [type_operation, reseau_id, pdv_id, montant_du, initiateur_type, initiateur_id, canal_deplacement || 'DIRECT_SIEGE']
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/operations', async (req, res) => {
  const result = await pool.query(`
    SELECT
      o.id, o.type_operation, o.montant_du, o.statut, o.date_operation, o.date_limite_reglement,
      o.initiateur_type, o.initiateur_id, o.canal_deplacement,
      p.id AS pdv_id, p.nom AS pdv_nom,
      r.id AS reseau_id, r.nom AS reseau_nom,
      COALESCE(SUM(v.montant), 0) AS montant_verse,
      o.montant_du - COALESCE(SUM(v.montant), 0) AS montant_restant
    FROM operations o
    JOIN pdv p ON p.id = o.pdv_id
    JOIN reseaux r ON r.id = o.reseau_id
    LEFT JOIN versements v ON v.operation_id = o.id
    GROUP BY o.id, p.id, p.nom, r.id, r.nom
    ORDER BY o.date_operation DESC, o.created_at DESC
  `);
  res.json(result.rows);
});

// ---- VERSEMENTS ----
app.post('/versements', async (req, res) => {
  try {
    const { operation_id, mode, reseau_id, montant, saisi_par_type, saisi_par_id, note } = req.body;
    const result = await pool.query(
      `INSERT INTO versements (operation_id, mode, reseau_id, montant, saisi_par_type, saisi_par_id, note)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [operation_id, mode, mode === 'UV' ? reseau_id : null, montant, saisi_par_type || null, saisi_par_id || null, note || null]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/versements/:operation_id', async (req, res) => {
  const result = await pool.query(`
    SELECT v.*, r.nom AS reseau_nom
    FROM versements v
    LEFT JOIN reseaux r ON r.id = v.reseau_id
    WHERE v.operation_id = $1
    ORDER BY v.date_versement DESC
  `, [req.params.operation_id]);
  res.json(result.rows);
});

// ---- ALERTES ----
app.get('/alertes', async (req, res) => {
  const result = await pool.query(`
    SELECT
      o.id AS operation_id, o.type_operation, o.date_limite_reglement,
      p.nom AS pdv_nom, r.nom AS reseau_nom,
      o.montant_du,
      COALESCE(SUM(v.montant), 0) AS montant_verse,
      o.montant_du - COALESCE(SUM(v.montant), 0) AS montant_restant
    FROM operations o
    JOIN pdv p ON p.id = o.pdv_id
    JOIN reseaux r ON r.id = o.reseau_id
    LEFT JOIN versements v ON v.operation_id = o.id
    GROUP BY o.id, p.nom, r.nom
    HAVING o.montant_du - COALESCE(SUM(v.montant), 0) > 0
    ORDER BY o.date_limite_reglement ASC
  `);
  res.json(result.rows);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Serveur démarré sur le port ${PORT}`);
});
