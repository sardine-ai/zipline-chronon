package ai.chronon.online;

import ai.chronon.online.fetcher.Fetcher;

public class JavaJoinSchemaResponse {
    public String joinName;
    public String keySchema;
    public String valueSchema;
    public String schemaHash;

    public JavaJoinSchemaResponse(String joinName, String keySchema, String valueSchema, String schemaHash) {
        this.joinName = joinName;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.schemaHash = schemaHash;
    }

    public JavaJoinSchemaResponse(Fetcher.JoinSchemaResponse scalaResponse){
        this.joinName = scalaResponse.joinName();
        this.keySchema = scalaResponse.keySchema();
        this.valueSchema = scalaResponse.valueSchema();
        this.schemaHash = scalaResponse.schemaHash();
    }

    public Fetcher.JoinSchemaResponse toScala() {
        return new Fetcher.JoinSchemaResponse(
                joinName,
                keySchema,
                valueSchema,
                schemaHash);
    }
}
