const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

// Connexion à la base Neon (le mot de passe est lu depuis une variable d'environnement, jamais écrit ici)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Route de test : vérifier que le serveur et la base fonctionnent
app.get('/', async (req, res) => {
  res.json({ message: 'API GAMServices Mobile Money en ligne ✅' });
});

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
  const { nom, telephone, adresse_siege } = req.body;
  const result = await pool.query(
    'INSERT INTO masters (nom, telephone, adresse_siege) VALUES ($1,$2,$3) RETURNING *',
    [nom, telephone, adresse_siege]
  );
  res.json(result.rows[0]);
});

app.get('/masters', async (req, res) => {
  const result = await pool.query('SELECT * FROM masters ORDER BY created_at DESC');
  res.json(result.rows);
});

// ---- COMMERCIAUX ----
app.post('/commerciaux', async (req, res) => {
  const { nom, telephone } = req.body;
  const result = await pool.query(
    'INSERT INTO commerciaux (nom, telephone) VALUES ($1,$2) RETURNING *',
    [nom, telephone]
  );
  res.json(result.rows[0]);
});

app.get('/commerciaux', async (req, res) => {
  const result = await pool.query('SELECT * FROM commerciaux ORDER BY created_at DESC');
  res.json(result.rows);
});

// ---- ZONES ----
app.post('/zones', async (req, res) => {
  const { nom, commercial_id } = req.body;
  const result = await pool.query(
    'INSERT INTO zones (nom, commercial_id) VALUES ($1,$2) RETURNING *',
    [nom, commercial_id]
  );
  res.json(result.rows[0]);
});

app.get('/zones', async (req, res) => {
  const result = await pool.query('SELECT * FROM zones ORDER BY created_at DESC');
  res.json(result.rows);
});

// ---- PDV ----
app.post('/pdv', async (req, res) => {
  const { nom, telephone, zone_id, est_proche_siege } = req.body;
  const result = await pool.query(
    'INSERT INTO pdv (nom, telephone, zone_id, est_proche_siege) VALUES ($1,$2,$3,$4) RETURNING *',
    [nom, telephone, zone_id, est_proche_siege || false]
  );
  res.json(result.rows[0]);
});

app.get('/pdv', async (req, res) => {
  const result = await pool.query('SELECT * FROM pdv ORDER BY created_at DESC');
  res.json(result.rows);
});

// ---- OPERATIONS (créer une recharge : Vente UV ou Retour UV) ----
app.post('/operations', async (req, res) => {
  const { type_operation, reseau_id, pdv_id, montant_du, initiateur_type, initiateur_id, canal_deplacement } = req.body;
  const result = await pool.query(
    `INSERT INTO operations (type_operation, reseau_id, pdv_id, montant_du, initiateur_type, initiateur_id, canal_deplacement)
     VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
    [type_operation, reseau_id, pdv_id, montant_du, initiateur_type, initiateur_id, canal_deplacement || 'DIRECT_SIEGE']
  );
  res.json(result.rows[0]);
});

app.get('/operations', async (req, res) => {
  const result = await pool.query('SELECT * FROM v_operations_solde ORDER BY date_limite_reglement DESC');
  res.json(result.rows);
});

// ---- VERSEMENTS (ajouter un règlement : cash ou UV, sur une opération) ----
app.post('/versements', async (req, res) => {
  const { operation_id, mode, reseau_id, montant, saisi_par_type, saisi_par_id, note } = req.body;
  const result = await pool.query(
    `INSERT INTO versements (operation_id, mode, reseau_id, montant, saisi_par_type, saisi_par_id, note)
     VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
    [operation_id, mode, mode === 'UV' ? reseau_id : null, montant, saisi_par_type, saisi_par_id, note]
  );
  res.json(result.rows[0]);
});

// ---- ALERTES : PDV n'ayant pas soldé leur opération ----
app.get('/alertes', async (req, res) => {
  const result = await pool.query(`
    SELECT o.id AS operation_id, p.nom AS pdv_nom, o.montant_du, o.montant_verse, o.montant_restant, o.date_limite_reglement
    FROM v_operations_solde o
    JOIN pdv p ON p.id = o.pdv_id
    WHERE o.montant_restant > 0
    ORDER BY o.date_limite_reglement ASC
  `);
  res.json(result.rows);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Serveur démarré sur le port ${PORT}`);
});
