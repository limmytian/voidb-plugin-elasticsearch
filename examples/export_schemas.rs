use std::fs;
use std::path::Path;
use voidb_plugin_elasticsearch::elasticsearch_capabilities;

fn main() -> anyhow::Result<()> {
    let schemas_dir = Path::new("schemas");
    fs::create_dir_all(schemas_dir)?;

    let capabilities = elasticsearch_capabilities();

    // Export capability schemas
    for cap in &capabilities {
        let input_path = schemas_dir.join(format!("{}-input.schema.json", cap.id));
        let output_path = schemas_dir.join(format!("{}-output.schema.json", cap.id));

        fs::write(&input_path, serde_json::to_string_pretty(&cap.input_schema)? + "\n")?;
        fs::write(&output_path, serde_json::to_string_pretty(&cap.output_schema)? + "\n")?;
        println!("Exported schemas for capability: {}", cap.id);
    }

    // Export profile schema matching EsConfig
    let profile_schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "ElasticsearchConnectionProfile",
        "type": "object",
        "required": ["urls"],
        "properties": {
            "urls": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Elasticsearch cluster URL(s), e.g. [\"http://localhost:9200\"]"
            },
            "auth": {
                "type": "object",
                "required": ["type"],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["Basic", "ApiKey", "Bearer"]
                    },
                    "username": {
                        "type": "string",
                        "description": "Username for Basic auth"
                    },
                    "password": {
                        "type": "string",
                        "description": "Password for Basic auth"
                    },
                    "id": {
                        "type": "string",
                        "description": "API Key ID"
                    },
                    "api_key": {
                        "type": "string",
                        "description": "API Key secret"
                    },
                    "token": {
                        "type": "string",
                        "description": "Bearer token"
                    }
                }
            },
            "timeout": {
                "type": "integer",
                "minimum": 1,
                "default": 30,
                "description": "Request timeout in seconds"
            },
            "verify_ssl": {
                "type": "boolean",
                "default": true,
                "description": "Whether to verify TLS certificates"
            }
        },
        "additionalProperties": false
    });

    let profile_path = schemas_dir.join("profile.schema.json");
    fs::write(&profile_path, serde_json::to_string_pretty(&profile_schema)? + "\n")?;
    println!("Exported profile schema to {}", profile_path.display());

    Ok(())
}
