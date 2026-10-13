package io.github.panteliszara.warexis.browser;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import tools.jackson.databind.json.JsonMapper;

class FixtureConfigTests {
    @TempDir Path temp;

    private Path repository() throws Exception {
        var root = Files.createDirectory(temp.resolve("checkout"));
        Files.createFile(root.resolve(".git"));
        for (var name : List.of("order", "inventory", "fulfillment")) {
            var jar = FixtureConfig.jar(root, name);
            Files.createDirectories(jar.getParent());
            Files.createFile(jar);
        }
        return root;
    }

    private Path realm(boolean pkce, boolean directGrant) throws Exception {
        var realm = temp.resolve("realm.json");
        Files.writeString(realm, JsonMapper.builder().build().writeValueAsString(Map.of("realm", "warexis", "clients", List.of(
                Map.of("clientId", "warexis-operations", "publicClient", true, "standardFlowEnabled", true,
                        "defaultClientScopes", List.of("roles", "warexis-api"),
                        "redirectUris", List.of("http://127.0.0.1:5173/"), "webOrigins", List.of("http://127.0.0.1:5173"),
                        "directAccessGrantsEnabled", directGrant, "attributes", Map.of("pkce.code.challenge.method", pkce ? "S256" : "plain"))))));
        return realm;
    }

    @Test void acceptsPublicPkceRealmAndExternalRuntime() throws Exception {
        var root = repository();
        var runtime = temp.resolve("private/session/runtime.json");
        assertThat(FixtureConfig.validated(root, realm(true, false), runtime).runtime()).isEqualTo(runtime);
    }

    @Test void rejectsCredentialsWithinGitCheckout() throws Exception {
        var root = repository();
        assertThatThrownBy(() -> FixtureConfig.validated(root, realm(true, false), root.resolve("private/runtime.json")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("outside every Git checkout");
    }

    @Test void rejectsSymlinkedRuntimeDirectory() throws Exception {
        var actual = Files.createDirectory(temp.resolve("actual"));
        var link = Files.createSymbolicLink(temp.resolve("link"), actual);
        assertThatThrownBy(() -> FixtureConfig.requireOutsideGit(link.resolve("runtime.json")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("symlinks");
    }

    @Test void rejectsExistingRuntimeWithoutOverwriting() throws Exception {
        var root = repository();
        var runtime = temp.resolve("existing.json");
        Files.writeString(runtime, "user-owned");
        assertThatThrownBy(() -> FixtureConfig.validated(root, realm(true, false), runtime))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("already exists");
        assertThat(Files.readString(runtime)).isEqualTo("user-owned");
    }

    @Test void rejectsMissingExecutableService() throws Exception {
        var root = repository();
        Files.delete(FixtureConfig.jar(root, "inventory"));
        assertThatThrownBy(() -> FixtureConfig.validated(root, realm(true, false), temp.resolve("runtime.json")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("all three executable");
    }

    @Test void rejectsPlainPkceAndPasswordGrant() throws Exception {
        var root = repository();
        assertThatThrownBy(() -> FixtureConfig.validated(root, realm(false, false), temp.resolve("runtime.json")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("public PKCE");
        assertThatThrownBy(() -> FixtureConfig.validated(root, realm(true, true), temp.resolve("runtime.json")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("public PKCE");
    }
}
