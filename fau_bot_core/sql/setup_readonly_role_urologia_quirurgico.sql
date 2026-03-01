-- Ejecutar con un usuario con privilegios de administración.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fau_bot_ro') THEN
        CREATE ROLE fau_bot_ro LOGIN PASSWORD 'CHANGE_ME_FAU_BOT_RO';
    END IF;
END$$;

GRANT CONNECT ON DATABASE urologia_quirurgico TO fau_bot_ro;
GRANT USAGE ON SCHEMA public TO fau_bot_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO fau_bot_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO fau_bot_ro;
