# OARepo File Pipeline Server

A WSGI/uWSGI-compatible server for processing file pipelines with support for Crypt4GH encryption operations.

## Overview

The OARepo File Pipeline Server is a secure file processing service that executes configurable pipeline steps on files. It is designed to handle cryptographic operations on genomic data files encrypted with Crypt4GH format, providing a secure way to decrypt, re-encrypt, and validate encrypted files.

## Architecture

### Request Flow

1. Client creates a JWT token signed with repository private key
2. Client encrypts JWT using server's public RSA key (JWE)
3. Client stores encrypted token in Redis with unique token ID
4. Client sends HTTP request to `/pipeline/<token_id>`
5. Server retrieves and deletes token from Redis (single-use)
6. Server decrypts JWE using server private key
7. Server verifies JWT signature using repository public key
8. Server executes pipeline steps defined in token payload
9. Server streams processed file back to client

### Security Model

- All tokens are single-use and deleted after retrieval
- JWE encryption ensures only the server can read token contents
- JWT signature verification ensures token authenticity
- Token expiration (exp) and issued-at (iat) claims are validated
- 5-second leeway for clock skew

## Configuration

### Environment Variables

- `CONFIG_FILE`: Path to configuration file (default: `config.json`)
- `KEY_PROVIDER`: Key provider type - `http` or `local` (default: `local`)

### Configuration File Structure

The server uses a JSON configuration file (`config.json` by default) with two main sections:

```json
{
  "hsm_servers": {
    "default": "http://hsm-server:8080/key/x25519/",
    "backup": "http://backup-hsm:8080/key/x25519/"
  },
  "rsa_keys": {
    "server_private_key": "-----BEGIN PRIVATE KEY-----\n...",
    "server_public_key": "-----BEGIN PUBLIC KEY-----\n...",
    "repo_public_key": "-----BEGIN PUBLIC KEY-----\n..."
  }
}
```

#### HSM Servers

Crypt4GH key identifiers. The format depends on the `KEY_PROVIDER`:

**HTTP Provider** (production):

- URLs pointing to HSM servers that provide Crypt4GH keys
- Example: `"http://hsm-server:8080/key/x25519/"`

**Local Provider** (testing/development):

- `env:VARIABLE_NAME` - Load key from environment variable
- `/path/to/key.sec` - Load key from file
- Direct Crypt4GH key string

#### RSA Keys

Required RSA keys for JWT/JWE operations:

- `server_private_key`: Server's RSA private key (for decrypting JWE tokens)
- `server_public_key`: Server's RSA public key (shared with clients for encrypting tokens)
- `repo_public_key`: Repository's RSA public key (for verifying JWT signatures)

### Server Startup Validation

On startup, the server validates:

- RSA server private key exists
- RSA repository public key exists
- At least one Crypt4GH key is configured
- Redis connection is accessible

If validation fails, the server will not start and will log detailed error messages.

## Pipeline Steps

Pipeline steps are defined in the token payload and executed sequentially. Each step processes the output of the previous step.

### Available Steps

#### decrypt_crypt4gh

Decrypts a Crypt4GH encrypted file.

**Arguments:**

- `source_url`: URL to the encrypted file (first step only)
- `recipient_sec`: Crypt4GH private key (PEM format) of the recipient

**Example:**

```json
{
  "type": "decrypt_crypt4gh",
  "arguments": {
    "source_url": "https://storage.example.com/file.c4gh"
  }
}
```

#### add_recipient_crypt4gh

Adds a new recipient to an existing Crypt4GH encrypted file, allowing the new recipient to decrypt it.

**Arguments:**

- `source_url`: URL to the encrypted file (first step only)
- `recipient_pub`: Crypt4GH public key (PEM format) of the new recipient

**Example:**

```json
{
  "type": "add_recipient_crypt4gh",
  "arguments": {
    "source_url": "https://storage.example.com/file.c4gh",
    "recipient_pub": "-----BEGIN CRYPT4GH PUBLIC KEY-----\n..."
  }
}
```

#### validate_crypt4gh

Validates a Crypt4GH encrypted file by attempting to read its entire contents. Returns JSON response instead of file download.

**Arguments:**

- `source_url`: URL to the encrypted file (first step only)

**Returns:**

```json
{
  "valid": true,
  "error": null
}
```

Or if validation fails:

```json
{
  "valid": false,
  "error": "Error message describing the issue"
}
```

**Example:**

```json
{
  "type": "validate_crypt4gh",
  "arguments": {
    "source_url": "https://storage.example.com/file.c4gh"
  }
}
```

### Pipeline Example

A complete pipeline that adds a recipient and then decrypts:

```json
{
  "pipeline_steps": [
    {
      "type": "add_recipient_crypt4gh",
      "arguments": {
        "source_url": "https://storage.example.com/file.c4gh",
        "recipient_pub": "-----BEGIN CRYPT4GH PUBLIC KEY-----\n..."
      }
    },
    {
      "type": "decrypt_crypt4gh",
      "arguments": {
        "recipient_sec": "-----BEGIN CRYPT4GH PRIVATE KEY-----\n..."
      }
    }
  ]
}
```

## CLI Commands

The server provides CLI commands for managing configuration.

### HSM Server Management

List all configured Crypt4GH keys:

```bash
python -m oarepo_file_pipeline_server.cli hsm list
```

Add a new Crypt4GH key:

```bash
python -m oarepo_file_pipeline_server.cli hsm add <name> <url-or-identifier>
```

Update an existing key:

```bash
python -m oarepo_file_pipeline_server.cli hsm update <name> <url-or-identifier>
```

Remove a key:

```bash
python -m oarepo_file_pipeline_server.cli hsm remove <name>
```

Reload configuration from file:

```bash
python -m oarepo_file_pipeline_server.cli hsm reload
```

Initialize new configuration file:

```bash
python -m oarepo_file_pipeline_server.cli hsm init [--force]
```

### RSA Key Management

List all configured RSA keys:

```bash
python -m oarepo_file_pipeline_server.cli rsa list
```

Add or update an RSA key from PEM file:

```bash
python -m oarepo_file_pipeline_server.cli rsa add <key-name> <path-to-pem-file>
```

Remove an RSA key:

```bash
python -m oarepo_file_pipeline_server.cli rsa remove <key-name>
```

Show an RSA key:

```bash
python -m oarepo_file_pipeline_server.cli rsa show <key-name>
```

### CLI Options

Most HSM commands support `--no-reinit` flag to prevent reinitialization of the KeyCollection after modification. This is useful when making multiple changes:

```bash
python -m oarepo_file_pipeline_server.cli hsm add server1 http://hsm1:8080/key/ --no-reinit
python -m oarepo_file_pipeline_server.cli hsm add server2 http://hsm2:8080/key/ --no-reinit
python -m oarepo_file_pipeline_server.cli hsm reload
```

## Deployment

## TODO: Docker documentation

## Request Processing Details

### Token Payload Structure

The JWT token must contain:

```json
{
  "iat": 1234567890,
  "exp": 1234567900,
  "pipeline_steps": [
    {
      "type": "step_type",
      "arguments": {
        "source_url": "https://...",
        ...
      }
    }
  ]
}
```

### URL Pattern

```
GET /pipeline/<token_id>
```

Where `token_id` is the Redis key containing the encrypted JWE token.

### Response Types

**File Download:**

- Content-Type: application/octet-stream (or appropriate MIME type)
- Content-Disposition: attachment; filename="output"
- Body: Streamed file content

**JSON Response** (validation step):

- Content-Type: application/json
- Body: JSON object with validation results

### Error Responses

**400 Bad Request:**

- Invalid request path
- Pipeline validation errors (no steps, invalid step types, etc.)

**404 Not Found:**

- Token not found or already used
- Token expired

**500 Internal Server Error:**

- Configuration errors (missing keys)
- Pipeline processing errors
- Unexpected errors

**503 Service Unavailable:**

- Server initialization failed
- Redis connection errors

## Development and Testing

### Testing Configuration

For testing, use the local key provider with environment variables:

```bash
export KEY_PROVIDER=local
export CONFIG_FILE=config_test.json
```

Example test configuration:

```json
{
  "hsm_servers": {
    "default": "env:SERVER_PRIVATE_KEY_C4GH"
  },
  "rsa_keys": {
    "server_private_key": "...",
    "server_public_key": "...",
    "repo_public_key": "..."
  }
}
```
