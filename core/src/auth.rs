// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::RequestBuilder;
use serde::Serialize;

use crate::error::{Error, Result};

pub trait Auth: Sync + Send {
    fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder>;
    fn can_reload(&self) -> bool {
        false
    }
    fn username(&self) -> String;
}

#[derive(Clone)]
pub struct BasicAuth {
    username: String,
    password: SensitiveString,
}

impl BasicAuth {
    pub fn new(username: impl ToString, password: impl ToString) -> Self {
        Self {
            username: username.to_string(),
            password: SensitiveString(password.to_string()),
        }
    }
}

impl Auth for BasicAuth {
    fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        Ok(builder.basic_auth(&self.username, Some(self.password.inner())))
    }

    fn username(&self) -> String {
        self.username.clone()
    }
}

#[derive(Clone)]
pub struct AccessTokenAuth {
    token: SensitiveString,
}

impl AccessTokenAuth {
    pub fn new(token: impl ToString) -> Self {
        Self {
            token: SensitiveString::from(token.to_string()),
        }
    }
}

impl Auth for AccessTokenAuth {
    fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        Ok(builder.bearer_auth(self.token.inner()))
    }

    fn username(&self) -> String {
        "token".to_string()
    }
}

#[derive(Clone)]
pub struct AccessTokenFileAuth {
    token_file: String,
}

impl AccessTokenFileAuth {
    pub fn new(token_file: impl ToString) -> Self {
        let token_file = token_file.to_string();
        Self { token_file }
    }
}

impl Auth for AccessTokenFileAuth {
    fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        let token = std::fs::read_to_string(&self.token_file).map_err(|e| {
            Error::IO(format!(
                "cannot read access token from file {}: {}",
                self.token_file, e
            ))
        })?;
        Ok(builder.bearer_auth(token.trim()))
    }

    fn can_reload(&self) -> bool {
        true
    }

    fn username(&self) -> String {
        "token".to_string()
    }
}

const HEADER_AUTH_METHOD: &str = "X-DATABEND-AUTH-METHOD";
const KEYPAIR_TOKEN_TTL_SECS: u64 = 60;

#[derive(Serialize)]
struct KeyPairClaims {
    sub: String,
    iat: u64,
    exp: u64,
}

#[derive(Clone)]
pub struct KeyPairAuth {
    username: String,
    encoding_key: Arc<EncodingKey>,
    algorithm: Algorithm,
}

impl KeyPairAuth {
    pub fn new(
        username: impl ToString,
        private_key_file: &str,
        passphrase_file: Option<&str>,
    ) -> Result<Self> {
        let pem_data = std::fs::read(private_key_file).map_err(|e| {
            Error::IO(format!(
                "cannot read private key from file {}: {}",
                private_key_file, e
            ))
        })?;

        let passphrase = match passphrase_file {
            Some(path) => {
                let p = std::fs::read_to_string(path).map_err(|e| {
                    Error::IO(format!("cannot read passphrase from file {}: {}", path, e))
                })?;
                Some(p.trim().to_string())
            }
            None => None,
        };

        let (encoding_key, algorithm) = Self::parse_private_key(&pem_data, passphrase.as_deref())?;

        Ok(Self {
            username: username.to_string(),
            encoding_key: Arc::new(encoding_key),
            algorithm,
        })
    }

    fn parse_private_key(
        pem_data: &[u8],
        passphrase: Option<&str>,
    ) -> Result<(EncodingKey, Algorithm)> {
        let pem_str = std::str::from_utf8(pem_data)
            .map_err(|e| Error::IO(format!("private key is not valid UTF-8: {e}")))?;

        if let Some(passphrase) = passphrase {
            // Encrypted PKCS#8 key — decrypt using pkcs8 crate to get DER
            Self::parse_encrypted_key(pem_str, passphrase)
        } else {
            // Unencrypted key — detect type and use jsonwebtoken's PEM methods
            Self::parse_unencrypted_key(pem_data, pem_str)
        }
    }

    fn parse_encrypted_key(pem_str: &str, passphrase: &str) -> Result<(EncodingKey, Algorithm)> {
        use pkcs8::DecodePrivateKey;

        let doc = pkcs8::SecretDocument::from_pkcs8_encrypted_pem(pem_str, passphrase.as_bytes())
            .map_err(|e| Error::IO(format!("failed to decrypt private key: {e}")))?;

        let der_bytes = doc.as_bytes();

        // Try each key type with DER
        // from_*_der returns EncodingKey directly (infallible for the struct construction),
        // but the underlying parsing may still fail at sign time.
        // We try RSA first, then EC, then Ed25519 by attempting to parse the key info.
        // Since from_*_der doesn't validate, we use the OID from the PKCS#8 structure.
        let private_key_info = pkcs8::PrivateKeyInfoRef::try_from(der_bytes)
            .map_err(|e| Error::IO(format!("failed to parse PKCS#8 DER: {e}")))?;

        let algorithm_oid = private_key_info.algorithm.oid;

        // RSA: 1.2.840.113549.1.1.1
        const RSA_OID: pkcs8::ObjectIdentifier =
            pkcs8::ObjectIdentifier::new_unwrap("1.2.840.113549.1.1.1");
        // EC: 1.2.840.10045.2.1
        const EC_OID: pkcs8::ObjectIdentifier =
            pkcs8::ObjectIdentifier::new_unwrap("1.2.840.10045.2.1");
        // Ed25519: 1.3.101.112
        const ED25519_OID: pkcs8::ObjectIdentifier =
            pkcs8::ObjectIdentifier::new_unwrap("1.3.101.112");

        if algorithm_oid == RSA_OID {
            // Ring's from_der expects PKCS#1 RSAPrivateKey DER, not full PKCS#8.
            // Extract the inner private key bytes from PKCS#8 PrivateKeyInfo.
            let rsa_der = private_key_info.private_key.as_bytes();
            Ok((EncodingKey::from_rsa_der(rsa_der), Algorithm::RS256))
        } else if algorithm_oid == EC_OID {
            let (ec_der, algorithm) = Self::rebuild_ec_pkcs8_named_curve(private_key_info)?;
            Ok((EncodingKey::from_ec_der(&ec_der), algorithm))
        } else if algorithm_oid == ED25519_OID {
            Ok((EncodingKey::from_ed_der(der_bytes), Algorithm::EdDSA))
        } else {
            Err(Error::IO(format!(
                "unsupported key algorithm OID: {algorithm_oid}"
            )))
        }
    }

    /// Rebuild a named-curve PKCS#8 DER for supported EC keys.
    ///
    /// jsonwebtoken/ring supports ES256 and ES384. Reject explicit parameters or
    /// unsupported curves instead of silently relabeling them with the wrong JWT alg.
    fn rebuild_ec_pkcs8_named_curve(pki: pkcs8::PrivateKeyInfoRef) -> Result<(Vec<u8>, Algorithm)> {
        use pkcs8::der::Encode;

        let (curve_oid, algorithm) = Self::ec_curve_oid_to_algorithm(
            pki.algorithm
                .parameters
                .and_then(|params| params.decode_as::<pkcs8::ObjectIdentifier>().ok()),
        )?;

        let alg_id = pkcs8::AlgorithmIdentifierRef {
            oid: pkcs8::ObjectIdentifier::new_unwrap("1.2.840.10045.2.1"),
            parameters: Some(pkcs8::der::asn1::AnyRef::from(&curve_oid)),
        };

        let new_pki = pkcs8::PrivateKeyInfo {
            algorithm: alg_id,
            private_key: pki.private_key,
            public_key: pki.public_key,
        };

        let der = new_pki
            .to_der()
            .map_err(|e| Error::IO(format!("failed to re-encode EC PKCS#8: {e}")))?;
        Ok((der, algorithm))
    }

    fn ec_curve_oid_to_algorithm(
        curve_oid: Option<pkcs8::ObjectIdentifier>,
    ) -> Result<(pkcs8::ObjectIdentifier, Algorithm)> {
        const P256_OID: pkcs8::ObjectIdentifier =
            pkcs8::ObjectIdentifier::new_unwrap("1.2.840.10045.3.1.7");
        const P384_OID: pkcs8::ObjectIdentifier =
            pkcs8::ObjectIdentifier::new_unwrap("1.3.132.0.34");

        match curve_oid {
            Some(P256_OID) => Ok((P256_OID, Algorithm::ES256)),
            Some(P384_OID) => Ok((P384_OID, Algorithm::ES384)),
            Some(curve_oid) => Err(Error::IO(format!(
                "unsupported EC private key curve OID: {curve_oid}; supported curves are P-256 and P-384"
            ))),
            None => Err(Error::IO(
                "unsupported EC private key parameters: expected named P-256 or P-384 curve"
                    .to_string(),
            )),
        }
    }

    fn parse_sec1_ec_algorithm(pem_data: &[u8]) -> Result<Algorithm> {
        use sec1::der::Decode;

        let pem =
            pem::parse(pem_data).map_err(|e| Error::IO(format!("failed to parse PEM: {e}")))?;
        let ec_key = sec1::EcPrivateKey::from_der(pem.contents())
            .map_err(|e| Error::IO(format!("failed to parse EC private key: {e}")))?;

        let curve_oid = ec_key
            .parameters
            .and_then(|params| params.named_curve())
            .ok_or_else(|| {
                Error::IO(
                    "unsupported EC private key parameters: expected named P-256 or P-384 curve"
                        .to_string(),
                )
            })?;

        match curve_oid.to_string().as_str() {
            "1.2.840.10045.3.1.7" => Ok(Algorithm::ES256),
            "1.3.132.0.34" => Ok(Algorithm::ES384),
            _ => Err(Error::IO(format!(
                "unsupported EC private key curve OID: {curve_oid}; supported curves are P-256 and P-384"
            ))),
        }
    }

    fn parse_unencrypted_key(pem_data: &[u8], pem_str: &str) -> Result<(EncodingKey, Algorithm)> {
        if pem_str.contains("RSA PRIVATE KEY") {
            // PKCS#1 RSA key
            let key = EncodingKey::from_rsa_pem(pem_data)
                .map_err(|e| Error::IO(format!("failed to parse RSA private key: {e}")))?;
            return Ok((key, Algorithm::RS256));
        }

        if pem_str.contains("EC PRIVATE KEY") {
            use pkcs8::der::Encode;

            // SEC1 EC key. Choose the JWT alg from the curve instead of
            // advertising the wrong alg for non-P-256 keys.
            let algorithm = Self::parse_sec1_ec_algorithm(pem_data)?;
            let pem_parsed =
                pem::parse(pem_data).map_err(|e| Error::IO(format!("failed to parse PEM: {e}")))?;
            let pkcs8_der = pkcs8::PrivateKeyInfo {
                algorithm: pkcs8::AlgorithmIdentifierRef {
                    oid: pkcs8::ObjectIdentifier::new_unwrap("1.2.840.10045.2.1"),
                    parameters: Some(pkcs8::der::asn1::AnyRef::from(&match algorithm {
                        Algorithm::ES256 => {
                            pkcs8::ObjectIdentifier::new_unwrap("1.2.840.10045.3.1.7")
                        }
                        Algorithm::ES384 => pkcs8::ObjectIdentifier::new_unwrap("1.3.132.0.34"),
                        _ => unreachable!(),
                    })),
                },
                private_key: pkcs8::der::asn1::OctetStringRef::new(pem_parsed.contents())
                    .map_err(|e| Error::IO(format!("failed to wrap EC private key: {e}")))?,
                public_key: None::<pkcs8::der::asn1::BitStringRef<'_>>,
            }
            .to_der()
            .map_err(|e| Error::IO(format!("failed to re-encode EC PKCS#8: {e}")))?;
            return Ok((EncodingKey::from_ec_der(&pkcs8_der), algorithm));
        }

        // PKCS#8 "BEGIN PRIVATE KEY" — parse OID to determine key type,
        // then use from_*_der with full PKCS#8 DER (from_ec_pem has issues with PKCS#8 EC keys)
        let pem_parsed =
            pem::parse(pem_data).map_err(|e| Error::IO(format!("failed to parse PEM: {e}")))?;
        let der_bytes = pem_parsed.contents();

        let private_key_info = pkcs8::PrivateKeyInfoRef::try_from(der_bytes)
            .map_err(|e| Error::IO(format!("failed to parse PKCS#8 DER: {e}")))?;

        let algorithm_oid = private_key_info.algorithm.oid;

        const RSA_OID: pkcs8::ObjectIdentifier =
            pkcs8::ObjectIdentifier::new_unwrap("1.2.840.113549.1.1.1");
        const EC_OID: pkcs8::ObjectIdentifier =
            pkcs8::ObjectIdentifier::new_unwrap("1.2.840.10045.2.1");
        const ED25519_OID: pkcs8::ObjectIdentifier =
            pkcs8::ObjectIdentifier::new_unwrap("1.3.101.112");

        if algorithm_oid == RSA_OID {
            // Ring's from_der expects PKCS#1 RSAPrivateKey DER, not full PKCS#8.
            // Extract the inner private key bytes from PKCS#8 PrivateKeyInfo.
            let rsa_der = private_key_info.private_key.as_bytes();
            Ok((EncodingKey::from_rsa_der(rsa_der), Algorithm::RS256))
        } else if algorithm_oid == EC_OID {
            let (ec_der, algorithm) = Self::rebuild_ec_pkcs8_named_curve(private_key_info)?;
            Ok((EncodingKey::from_ec_der(&ec_der), algorithm))
        } else if algorithm_oid == ED25519_OID {
            Ok((EncodingKey::from_ed_der(der_bytes), Algorithm::EdDSA))
        } else {
            Err(Error::IO(format!(
                "unsupported key algorithm OID: {algorithm_oid}"
            )))
        }
    }

    fn generate_jwt(&self) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::IO(format!("system time error: {e}")))?
            .as_secs();

        let claims = KeyPairClaims {
            sub: self.username.clone(),
            iat: now,
            exp: now + KEYPAIR_TOKEN_TTL_SECS,
        };

        let header = Header::new(self.algorithm);
        encode(&header, &claims, &self.encoding_key)
            .map_err(|e| Error::IO(format!("failed to sign JWT: {e}")))
    }
}

impl Auth for KeyPairAuth {
    fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        let token = self.generate_jwt()?;
        Ok(builder
            .bearer_auth(token)
            .header(HEADER_AUTH_METHOD, "keypair"))
    }

    fn can_reload(&self) -> bool {
        true
    }

    fn username(&self) -> String {
        self.username.clone()
    }
}

#[derive(::serde::Deserialize, ::serde::Serialize)]
#[serde(from = "String", into = "String")]
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SensitiveString(String);

impl From<String> for SensitiveString {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SensitiveString {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<SensitiveString> for String {
    fn from(value: SensitiveString) -> Self {
        value.0
    }
}

impl std::fmt::Display for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "**REDACTED**")
    }
}

impl std::fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // we keep the double quotes here to keep the String behavior
        write!(f, "\"**REDACTED**\"")
    }
}

impl SensitiveString {
    #[must_use]
    pub fn inner(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization() {
        let json_value = "\"foo\"";
        let value: SensitiveString = serde_json::from_str(json_value).unwrap();
        let result: String = serde_json::to_string(&value).unwrap();
        assert_eq!(result, json_value);
    }

    #[test]
    fn hide_content() {
        let value = SensitiveString("hello world".to_string());
        let display = format!("{value}");
        assert_eq!(display, "**REDACTED**");
        let debug = format!("{value:?}");
        assert_eq!(debug, "\"**REDACTED**\"");
    }

    #[test]
    fn keypair_auth_rsa() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Generate a test RSA private key in PKCS#8 format using openssl command
        // Note: `openssl genrsa` outputs PKCS#1 which may have compatibility issues
        // with some versions of ring. Using genpkey ensures PKCS#8 format.
        let output = std::process::Command::new("openssl")
            .args([
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
            ])
            .output();
        let output = match output {
            Ok(o) if o.status.success() => o,
            _ => {
                // Skip test if openssl is not available
                return;
            }
        };

        let mut key_file = NamedTempFile::new().unwrap();
        key_file.write_all(&output.stdout).unwrap();

        let auth = KeyPairAuth::new("testuser", key_file.path().to_str().unwrap(), None).unwrap();
        assert_eq!(auth.username(), "testuser");
        assert!(auth.can_reload());
        assert_eq!(auth.algorithm, Algorithm::RS256);

        // Verify JWT can be generated
        let token = auth.generate_jwt().unwrap();
        assert!(!token.is_empty());

        // Verify JWT structure (header.payload.signature)
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn keypair_auth_rsa_pkcs1() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Generate a PKCS#1 RSA key (BEGIN RSA PRIVATE KEY)
        let output = std::process::Command::new("openssl")
            .args(["genrsa", "2048"])
            .output();
        let output = match output {
            Ok(o) if o.status.success() => o,
            _ => return,
        };

        let pem_str = String::from_utf8_lossy(&output.stdout);
        if !pem_str.contains("RSA PRIVATE KEY") {
            // Skip if openssl outputs PKCS#8 format instead
            return;
        }

        let mut key_file = NamedTempFile::new().unwrap();
        key_file.write_all(&output.stdout).unwrap();

        let auth = KeyPairAuth::new("testuser", key_file.path().to_str().unwrap(), None).unwrap();
        assert_eq!(auth.algorithm, Algorithm::RS256);

        let token = auth.generate_jwt().unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
    }

    fn gen_sec1_ec_private_key(curve: &str) -> Option<Vec<u8>> {
        let output = std::process::Command::new("openssl")
            .args(["ecparam", "-name", curve, "-genkey", "-noout"])
            .output()
            .ok()?;
        output.status.success().then_some(output.stdout)
    }

    fn gen_ec_private_key(curve: &str, encrypted: bool) -> Option<Vec<u8>> {
        let mut args = vec![
            "genpkey",
            "-algorithm",
            "EC",
            "-pkeyopt",
            curve,
            "-pkeyopt",
            "ec_param_enc:named_curve",
        ];
        if encrypted {
            args.extend([
                "-aes-256-cbc",
                "-pass",
                "pass:testpass",
                "-v2prf",
                "hmacWithSHA256",
            ]);
        }

        let output = std::process::Command::new("openssl")
            .args(args)
            .output()
            .ok()?;
        output.status.success().then_some(output.stdout)
    }

    #[test]
    fn keypair_auth_ec() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let Some(key) = gen_ec_private_key("ec_paramgen_curve:P-256", false) else {
            return;
        };

        let mut key_file = NamedTempFile::new().unwrap();
        key_file.write_all(&key).unwrap();

        let auth = KeyPairAuth::new("testuser", key_file.path().to_str().unwrap(), None).unwrap();
        assert_eq!(auth.algorithm, Algorithm::ES256);

        let token = auth.generate_jwt().unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn keypair_auth_ec_p384() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let Some(key) = gen_ec_private_key("ec_paramgen_curve:P-384", false) else {
            return;
        };

        let mut key_file = NamedTempFile::new().unwrap();
        key_file.write_all(&key).unwrap();

        let auth = KeyPairAuth::new("testuser", key_file.path().to_str().unwrap(), None).unwrap();
        assert_eq!(auth.algorithm, Algorithm::ES384);

        let token = auth.generate_jwt().unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn keypair_auth_sec1_ec_p384() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let Some(key) = gen_sec1_ec_private_key("secp384r1") else {
            return;
        };

        let mut key_file = NamedTempFile::new().unwrap();
        key_file.write_all(&key).unwrap();

        let auth = KeyPairAuth::new("testuser", key_file.path().to_str().unwrap(), None).unwrap();
        assert_eq!(auth.algorithm, Algorithm::ES384);

        let token = auth.generate_jwt().unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn keypair_auth_encrypted_ec_p384() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let Some(key) = gen_ec_private_key("ec_paramgen_curve:P-384", true) else {
            return;
        };

        let mut key_file = NamedTempFile::new().unwrap();
        key_file.write_all(&key).unwrap();

        let mut pass_file = NamedTempFile::new().unwrap();
        pass_file.write_all(b"testpass\n").unwrap();

        let auth = KeyPairAuth::new(
            "testuser",
            key_file.path().to_str().unwrap(),
            Some(pass_file.path().to_str().unwrap()),
        )
        .unwrap();
        assert_eq!(auth.algorithm, Algorithm::ES384);

        let token = auth.generate_jwt().unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn keypair_auth_rejects_unsupported_ec_curves() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let Some(key) = gen_ec_private_key("ec_paramgen_curve:P-521", false) else {
            return;
        };

        let mut key_file = NamedTempFile::new().unwrap();
        key_file.write_all(&key).unwrap();

        let err = KeyPairAuth::new("testuser", key_file.path().to_str().unwrap(), None)
            .err()
            .expect("unsupported EC curve should be rejected");
        assert!(
            err.to_string()
                .contains("unsupported EC private key curve OID"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn keypair_auth_ed25519() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Generate a test Ed25519 private key
        let output = std::process::Command::new("openssl")
            .args(["genpkey", "-algorithm", "ed25519"])
            .output();
        let output = match output {
            Ok(o) if o.status.success() => o,
            _ => return,
        };

        let mut key_file = NamedTempFile::new().unwrap();
        key_file.write_all(&output.stdout).unwrap();

        let auth = KeyPairAuth::new("testuser", key_file.path().to_str().unwrap(), None).unwrap();
        assert_eq!(auth.algorithm, Algorithm::EdDSA);

        let token = auth.generate_jwt().unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn keypair_auth_encrypted_key() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Generate an encrypted RSA private key with scrypt KDF (supported by pkcs8 crate)
        let output = std::process::Command::new("openssl")
            .args([
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-aes-256-cbc",
                "-pass",
                "pass:testpass",
                "-v2prf",
                "hmacWithSHA256",
            ])
            .output();
        let output = match output {
            Ok(o) if o.status.success() => o,
            _ => return,
        };

        // Check if the generated key is actually encrypted
        let pem_str = String::from_utf8_lossy(&output.stdout);
        if !pem_str.contains("ENCRYPTED") {
            return;
        }

        let mut key_file = NamedTempFile::new().unwrap();
        key_file.write_all(&output.stdout).unwrap();

        let mut pass_file = NamedTempFile::new().unwrap();
        pass_file.write_all(b"testpass\n").unwrap();

        let auth = KeyPairAuth::new(
            "testuser",
            key_file.path().to_str().unwrap(),
            Some(pass_file.path().to_str().unwrap()),
        )
        .unwrap();
        assert_eq!(auth.algorithm, Algorithm::RS256);

        let token = auth.generate_jwt().unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
    }
}
