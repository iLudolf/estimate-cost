#!/usr/bin/env node
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const tsxCli = fileURLToPath(import.meta.resolve('tsx/cli'));

const child = spawn(
  process.execPath,
  [
    tsxCli,
    path.join(__dirname, '..', 'src', 'embedding-cli.ts'),
    ...process.argv.slice(2),
  ],
  {
    stdio: 'inherit',
    env: process.env,
  }
);

child.on('exit', (code) => process.exit(code ?? 0));
