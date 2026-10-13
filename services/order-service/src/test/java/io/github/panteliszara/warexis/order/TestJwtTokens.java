package io.github.panteliszara.warexis.order;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

import io.github.panteliszara.warexis.order.configuration.SecurityConfiguration;

final class TestJwtTokens {
    static final String ISSUER = "https://issuer.example.test/realms/warexis";
    static final String AUDIENCE = "warexis-api";
    private static final RSAKey KEY = key();

    static JwtDecoder decoder() throws JOSEException {
        var decoder = NimbusJwtDecoder.withPublicKey(KEY.toRSAPublicKey()).build();
        decoder.setJwtValidator(org.springframework.security.oauth2.jwt.JwtValidators.createDefaultWithValidators(
                new SecurityConfiguration().requiredTimeClaimsValidator(ISSUER, AUDIENCE),
                new org.springframework.security.oauth2.jwt.JwtIssuerValidator(ISSUER),
                new org.springframework.security.oauth2.jwt.JwtAudienceValidator(AUDIENCE)));
        return decoder;
    }

    static String operator() { return token("WAREXIS_OPERATOR", claims -> {}); }
    static String viewer() { return token("WAREXIS_VIEWER", claims -> {}); }

    static String token(String role, Consumer<JWTClaimsSet.Builder> customize) {
        return token(role, customize, KEY);
    }

    static String badSignature() { return token("WAREXIS_OPERATOR", claims -> {}, key()); }

    private static String token(String role, Consumer<JWTClaimsSet.Builder> customize, RSAKey key) {
        try {
            var now = Instant.now();
            var claims = new JWTClaimsSet.Builder().issuer(ISSUER).subject("test-operator")
                    .audience(AUDIENCE).issueTime(Date.from(now))
                    .notBeforeTime(Date.from(now.minusSeconds(1))).expirationTime(Date.from(now.plusSeconds(300)))
                    .claim("realm_access", Map.of("roles", List.of(role)));
            customize.accept(claims);
            var jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(), claims.build());
            jwt.sign(new RSASSASigner(key));
            return jwt.serialize();
        } catch (JOSEException exception) {
            throw new IllegalStateException("Could not create test credential", exception);
        }
    }

    private static RSAKey key() {
        try { return new RSAKeyGenerator(2048).keyID("ephemeral-test-key").generate(); }
        catch (JOSEException exception) { throw new IllegalStateException("Could not create test key", exception); }
    }
}
