"""
EndDec util
"""
import os
import string
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class EncDec:
    """
    EndDec util
    """

    def __init__(self):
        # Base62 alphabet: only alphanumeric characters.
        self.BASE62_ALPHABET = string.digits + string.ascii_uppercase + string.ascii_lowercase  # 0-9, A-Z, a-z

    def base62_encode(self, data: bytes) -> str:
        """
        EndDec util
        """
        num = int.from_bytes(data, 'big')
        if num == 0:
            return self.BASE62_ALPHABET[0]
        base = len(self.BASE62_ALPHABET)
        encoded = []
        while num:
            num, rem = divmod(num, base)
            encoded.append(self.BASE62_ALPHABET[rem])
        encoded.reverse()
        return ''.join(encoded)

    def base62_decode(self, s: str) -> bytes:
        """
        EndDec util
        """
        base = len(self.BASE62_ALPHABET)
        num = 0
        for char in s:
            num = num * base + self.BASE62_ALPHABET.index(char)
        # Calculate the number of bytes needed.
        byte_length = (num.bit_length() + 7) // 8
        return num.to_bytes(byte_length, 'big')

    def derive_key(self, secret: str, salt: bytes) -> bytes:
        """
        EndDec util
        """
        # Derive a 32-byte key using PBKDF2HMAC (AES-256 needs 32 bytes).
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return kdf.derive(secret.encode())

    def encrypt_base62(self, plaintext: str, secret: str) -> str:
        """
        EndDec util
        """
        # Generate a random 16-byte salt.
        salt = os.urandom(16)
        key = self.derive_key(secret, salt)
        aesgcm = AESGCM(key)
        # Generate a random 12-byte nonce (recommended for AESGCM).
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, plaintext.encode(), None)

        # Combine salt + nonce + ciphertext.
        encrypted_bytes = salt + nonce + ciphertext

        # Encode using Base62.
        encrypted_b62 = self.base62_encode(encrypted_bytes)

        # Enforce the length constraint.
        if len(encrypted_b62) >= 255:
            raise ValueError("Encrypted string exceeds the length limit of 255 characters.")

        return encrypted_b62

    def decrypt_base62(self, encrypted_b62: str, secret: str) -> str:
        """
        EndDec util
        """
        encrypted_bytes = self.base62_decode(encrypted_b62)

        # Extract salt (first 16 bytes), nonce (next 12 bytes), and the ciphertext.
        salt = encrypted_bytes[:16]
        nonce = encrypted_bytes[16:28]
        ciphertext = encrypted_bytes[28:]
        key = self.derive_key(secret, salt)
        aesgcm = AESGCM(key)
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return plaintext.decode('utf-8')


if __name__ == '__main__':
    encrypted = EncDec().encrypt_base62("yunind7.tradier", "Xjyrmnfg@321")
    print(encrypted)
    print(EncDec().decrypt_base62(encrypted, "Xjyrmnfg@321"))
