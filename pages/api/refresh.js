import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';

export default async function handler(req, res) {
  try {
    // Run the Python script to regenerate the JSON
    execSync('python3 scripts/generate_game_cards.py');
    res.status(200).json({ success: true, message: 'game_cards_data.json regenerated.' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.toString() });
  }
}