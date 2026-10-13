package io.github.panteliszara.warexis.fulfillment;

import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.*;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.jwt.*;

@TestConfiguration(proxyBeanMethods = false)
class TestJwtConfiguration {
    static final String ISSUER = "https://issuer.example.test/realms/warexis";
    static final String AUDIENCE = "warexis-api";
    static final KeyPair KEY = key();
    @Bean JwtDecoder testJwtDecoder(List<OAuth2TokenValidator<Jwt>> additional) {
        var decoder = NimbusJwtDecoder.withPublicKey((RSAPublicKey) KEY.getPublic()).build();
        var validators = new ArrayList<OAuth2TokenValidator<Jwt>>(additional);
        validators.add(JwtValidators.createDefaultWithIssuer(ISSUER));
        validators.add(new JwtClaimValidator<List<String>>("aud", audience -> audience != null && audience.contains(AUDIENCE)));
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(validators));
        return decoder;
    }
    static String operator() { return token("WAREXIS_OPERATOR", claims -> {}); }
    static String token(String role, Consumer<JWTClaimsSet.Builder> customize) {
        var claims = new JWTClaimsSet.Builder().issuer(ISSUER).subject("ephemeral-service-test")
                .audience(AUDIENCE).issueTime(Date.from(Instant.now()))
                .expirationTime(Date.from(Instant.now().plusSeconds(600)))
                .claim("realm_access", Map.of("roles", List.of(role)));
        customize.accept(claims);
        return sign(claims.build(), KEY);
    }
    static String sign(JWTClaimsSet claims, KeyPair key) {
        try {
            var jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT).build(), claims);
            jwt.sign(new RSASSASigner(key.getPrivate())); return jwt.serialize();
        } catch (Exception failure) { throw new IllegalStateException("Could not sign ephemeral test credential", failure); }
    }
    static KeyPair key() {
        try { var generator = KeyPairGenerator.getInstance("RSA"); generator.initialize(2048); return generator.generateKeyPair(); }
        catch (Exception failure) { throw new IllegalStateException("Could not generate ephemeral test key", failure); }
    }
}
