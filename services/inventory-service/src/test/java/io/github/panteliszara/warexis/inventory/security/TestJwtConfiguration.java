package io.github.panteliszara.warexis.inventory.security;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.JwtAudienceValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

@TestConfiguration(proxyBeanMethods = false)
public class TestJwtConfiguration {
    public static final String ISSUER = "https://issuer.test/realms/warexis";
    public static final String AUDIENCE = "warexis-api";
    static final KeyPair KEY = key();

    @Bean
    JwtDecoder testJwtDecoder() {
        var decoder = NimbusJwtDecoder.withPublicKey((RSAPublicKey) KEY.getPublic()).build();
        var timestamps = new JwtTimestampValidator();
        timestamps.setAllowEmptyExpiryClaim(false);
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefaultWithIssuer(ISSUER), new JwtAudienceValidator(AUDIENCE), timestamps));
        return decoder;
    }

    public static String operatorToken() {
        return token(claims -> claims.claim("realm_access", Map.of("roles", List.of("WAREXIS_OPERATOR"))));
    }

    static String token(Consumer<JWTClaimsSet.Builder> customize) {
        return token(customize, KEY);
    }

    static String token(Consumer<JWTClaimsSet.Builder> customize, KeyPair key) {
        try {
            var now = Instant.now();
            var claims = new JWTClaimsSet.Builder().issuer(ISSUER).subject("test-operator")
                    .audience(AUDIENCE).issueTime(java.util.Date.from(now))
                    .expirationTime(java.util.Date.from(now.plusSeconds(300)));
            customize.accept(claims);
            var jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT).build(), claims.build());
            jwt.sign(new RSASSASigner(key.getPrivate()));
            return jwt.serialize();
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot create ephemeral test credential", exception);
        }
    }

    static KeyPair key() {
        try {
            var generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
