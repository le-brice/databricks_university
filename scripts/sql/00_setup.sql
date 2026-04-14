-- ============================================================
-- TU/e Demo: Setup Catalog & Schemas
-- Run this first before any other script.
-- ============================================================

CREATE CATALOG IF NOT EXISTS demo_tue;

CREATE SCHEMA IF NOT EXISTS demo_tue.raw;
CREATE SCHEMA IF NOT EXISTS demo_tue.staging;
CREATE SCHEMA IF NOT EXISTS demo_tue.intermediate;
CREATE SCHEMA IF NOT EXISTS demo_tue.marts;
