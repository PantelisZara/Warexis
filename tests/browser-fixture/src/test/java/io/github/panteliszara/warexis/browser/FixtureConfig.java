package io.github.panteliszara.warexis.browser;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import tools.jackson.databind.json.JsonMapper;

record FixtureConfig(Path repository, Path realm, Path runtime) {
    static FixtureConfig fromProperties() {
        return validated(Path.of(System.getProperty("fixture.repository", "../..")),
                Path.of(System.getProperty("fixture.realm", "../../infrastructure/auth/warexis-realm.json")),
                Path.of(System.getProperty("fixture.runtime", System.getProperty("user.home")
                        + "/.local/state/warexis-browser-fixture/runtime.json")));
    }

    static FixtureConfig validated(Path repository, Path realm, Path runtime) {
        try {
            repository = repository.toRealPath();
            realm = realm.toRealPath();
            runtime = runtime.toAbsolutePath().normalize();
            requireOutsideGit(runtime);
            if (Files.exists(runtime)) throw new IllegalArgumentException("Runtime file already exists; use a new private session path");
            var json = JsonMapper.builder().build().readTree(Files.readString(realm));
            if (!"warexis".equals(json.path("realm").asString())) throw new IllegalArgumentException("Fixture requires the warexis realm");
            boolean spa = false;
            for (var client : json.path("clients")) {
                if (!"warexis-operations".equals(client.path("clientId").asString())) continue;
                spa = client.path("publicClient").asBoolean() && client.path("standardFlowEnabled").asBoolean()
                        && !client.path("directAccessGrantsEnabled").asBoolean()
                        && !client.path("implicitFlowEnabled").asBoolean()
                        && !client.path("serviceAccountsEnabled").asBoolean()
                        && "S256".equals(client.path("attributes").path("pkce.code.challenge.method").asString())
                        && !client.path("fullScopeAllowed").asBoolean()
                        && contains(client.path("defaultClientScopes"), "roles")
                        && contains(client.path("defaultClientScopes"), "warexis-api")
                        && client.path("redirectUris").size() == 1
                        && contains(client.path("redirectUris"), "http://127.0.0.1:5173/")
                        && client.path("webOrigins").size() == 1
                        && contains(client.path("webOrigins"), "http://127.0.0.1:5173");
            }
            if (!spa) throw new IllegalArgumentException("Realm must contain the reviewed public PKCE warexis-operations client");
            for (String service : List.of("order", "inventory", "fulfillment")) {
                if (!Files.isRegularFile(jar(repository, service))) throw new IllegalArgumentException("Build all three executable service JARs first");
            }
            return new FixtureConfig(repository, realm, runtime);
        } catch (java.io.IOException failure) {
            throw new IllegalArgumentException("Cannot read fixture configuration", failure);
        }
    }

    private static boolean contains(tools.jackson.databind.JsonNode values, String expected) {
        if (!values.isArray()) return false;
        for (var value : values) if (expected.equals(value.asString())) return true;
        return false;
    }

    static Path jar(Path repository, String service) {
        return repository.resolve("services/" + service + "-service/target/" + service + "-service-0.0.1-SNAPSHOT.jar");
    }

    static void requireOutsideGit(Path file) {
        for (Path parent = file.toAbsolutePath().normalize(); parent != null; parent = parent.getParent()) {
            if (Files.isSymbolicLink(parent)) throw new IllegalArgumentException("Private runtime path must not contain symlinks");
            if (Files.exists(parent.resolve(".git"))) throw new IllegalArgumentException("Private runtime files must be outside every Git checkout");
        }
    }
}
