#!/usr/bin/env python3

import os
import base64
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from pathlib import Path


class EncryptionManager:
    
    def __init__(self, key_file=None, password=None):
        self.key_file = key_file
        self.key = None
        
        if key_file and os.path.exists(key_file):
            self.key = self._load_key(key_file)
        elif password:
            self.key = self._derive_key_from_password(password)
        else:
            self.key = self._generate_key()
            if key_file:
                self._save_key(key_file, self.key)
        
        self.cipher = AESGCM(self.key)
    
    def _generate_key(self):
        return AESGCM.generate_key(bit_length=256)
    
    def _derive_key_from_password(self, password):
        salt = b'backup_system_salt_v1'
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return kdf.derive(password.encode())
    
    def _save_key(self, key_file, key):
        os.makedirs(os.path.dirname(key_file), exist_ok=True)
        with open(key_file, 'wb') as f:
            f.write(base64.b64encode(key))
        os.chmod(key_file, 0o600)
        print(f"Encryption key saved to: {key_file}")
    
    def _load_key(self, key_file):
        with open(key_file, 'rb') as f:
            return base64.b64decode(f.read())
    
    def encrypt_data(self, data):
        nonce = os.urandom(12)
        if isinstance(data, str):
            data = data.encode()
        ciphertext = self.cipher.encrypt(nonce, data, None)
        return nonce + ciphertext
    
    def decrypt_data(self, encrypted_data):
        nonce = encrypted_data[:12]
        ciphertext = encrypted_data[12:]
        return self.cipher.decrypt(nonce, ciphertext, None)
    
    def encrypt_file(self, input_path, output_path=None):
        input_path = Path(input_path)
        
        if output_path is None:
            output_path = input_path.with_suffix(input_path.suffix + '.enc')
        else:
            output_path = Path(output_path)
        
        with open(input_path, 'rb') as f:
            plaintext = f.read()
        
        encrypted = self.encrypt_data(plaintext)
        
        with open(output_path, 'wb') as f:
            f.write(encrypted)
        
        return output_path
    
    def decrypt_file(self, input_path, output_path=None):
        input_path = Path(input_path)
        
        if output_path is None:
            if input_path.suffix == '.enc':
                output_path = input_path.with_suffix('')
            else:
                output_path = input_path.with_suffix('.dec')
        else:
            output_path = Path(output_path)
        
        with open(input_path, 'rb') as f:
            encrypted = f.read()
        
        decrypted = self.decrypt_data(encrypted)
        
        with open(output_path, 'wb') as f:
            f.write(decrypted)
        
        return output_path
    
    def encrypt_stream(self, input_stream):
        data = input_stream.read()
        return self.encrypt_data(data)
    
    def decrypt_stream(self, encrypted_data):
        return self.decrypt_data(encrypted_data)


def generate_encryption_key(key_file):
    manager = EncryptionManager(key_file=key_file)
    print(f"New encryption key generated and saved to: {key_file}")
    print(f"IMPORTANT: Keep this file secure and backed up separately!")
    return manager


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Generate key: python encryption.py generate <key_file>")
        print("  Encrypt file: python encryption.py encrypt <input_file> <key_file> [output_file]")
        print("  Decrypt file: python encryption.py decrypt <input_file> <key_file> [output_file]")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "generate":
        if len(sys.argv) < 3:
            print("Error: Please specify key file path")
            sys.exit(1)
        generate_encryption_key(sys.argv[2])
    
    elif command == "encrypt":
        if len(sys.argv) < 4:
            print("Error: Please specify input file and key file")
            sys.exit(1)
        
        input_file = sys.argv[2]
        key_file = sys.argv[3]
        output_file = sys.argv[4] if len(sys.argv) > 4 else None
        
        manager = EncryptionManager(key_file=key_file)
        output = manager.encrypt_file(input_file, output_file)
        print(f"File encrypted: {output}")
    
    elif command == "decrypt":
        if len(sys.argv) < 4:
            print("Error: Please specify input file and key file")
            sys.exit(1)
        
        input_file = sys.argv[2]
        key_file = sys.argv[3]
        output_file = sys.argv[4] if len(sys.argv) > 4 else None
        
        manager = EncryptionManager(key_file=key_file)
        output = manager.decrypt_file(input_file, output_file)
        print(f"File decrypted: {output}")
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
