\c mockdb;

DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users (username, email) VALUES 
('john_doe', 'john.doe@example.com'),
('jane_doe', 'jane.doe@example.com'),
('alice_smith', 'alice.smith@example.com'),
('bob_johnson', 'bob.johnson@example.com')
ON CONFLICT (username) DO NOTHING;
