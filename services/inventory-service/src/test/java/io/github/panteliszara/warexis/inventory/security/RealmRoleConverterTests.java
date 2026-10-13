package io.github.panteliszara.warexis.inventory.security;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RealmRoleConverterTests {
    @Test
    void mapsOnlyExactRealmBusinessRolesWithoutDuplicates() {
        var jwt = jwt(Map.of("roles", Arrays.asList("WAREXIS_OPERATOR", "WAREXIS_VIEWER", "WAREXIS_OPERATOR", 42, null,
                "ROLE_WAREXIS_OPERATOR", "WAREXIS_OPERATOR ", "warexis_viewer")));
        assertThat(new RealmRoleConverter().convert(jwt)).extracting("authority")
                .containsExactly("ROLE_WAREXIS_OPERATOR", "ROLE_WAREXIS_VIEWER");
    }

    @Test
    void malformedStructuresGrantNoPrivileges() {
        for (Object realm : List.of("WAREXIS_OPERATOR", List.of("WAREXIS_OPERATOR"), Map.of("roles", "WAREXIS_OPERATOR"), Map.of("roles", 5))) {
            assertThat(new RealmRoleConverter().convert(jwt(realm))).isEmpty();
        }
        assertThat(new RealmRoleConverter().convert(Jwt.withTokenValue("test").header("alg", "RS256").subject("test").build())).isEmpty();
    }

    @Test
    void missingOrBlankConfigurationFailsClosed() {
        assertThatThrownBy(() -> new SecurityConfiguration(" ", "warexis-api")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SecurityConfiguration("https://issuer.test", "")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void frameworkDefaultAllowsMissingExpiryAndOurAdditionalValidatorRejectsIt() {
        var jwt = Jwt.withTokenValue("test").header("alg", "RS256").subject("test").build();
        assertThat(new JwtTimestampValidator().validate(jwt).hasErrors()).isFalse();
        assertThat(new SecurityConfiguration("https://issuer.test", "warexis-api").requiredExpiry().validate(jwt).hasErrors()).isTrue();
    }

    private Jwt jwt(Object realm) {
        return Jwt.withTokenValue("test").header("alg", "RS256").subject("test")
                .claim("realm_access", realm).issuedAt(Instant.now()).expiresAt(Instant.now().plusSeconds(60)).build();
    }
}
