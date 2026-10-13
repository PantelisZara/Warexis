package io.github.panteliszara.warexis.fulfillment;

import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import com.nimbusds.jwt.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.oauth2.jwt.*;
import io.github.panteliszara.warexis.fulfillment.application.FulfillmentService;
import io.github.panteliszara.warexis.fulfillment.configuration.SecurityConfiguration;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties={"WAREXIS_OAUTH2_ISSUER_URI=https://issuer.example.test/realms/warexis", "WAREXIS_OAUTH2_AUDIENCE=warexis-api"})
@Import(FulfillmentServiceTests.PostgresConfiguration.class)
class SecurityTests {
    @LocalServerPort int port;
    @Autowired FulfillmentService service;
    @Autowired JdbcClient jdbc;

    @Test void healthIsAnonymousAndContainsNoDetails() throws Exception {
        var response=request("GET","/actuator/health",null,null);
        assertThat(response.statusCode()).isEqualTo(200);
        var health = tools.jackson.databind.json.JsonMapper.builder().build().readTree(response.body());
        assertThat(health.path("status").asString()).isEqualTo("UP");
        assertThat(health.has("components")).isFalse();
        assertThat(health.has("details")).isFalse();
        assertStateless(response);
    }

    @Test void missingTokenAndBasicAreUnauthorizedWithoutSessionOrLoginRedirect() throws Exception {
        for (var authorization : Arrays.asList(null,"Basic dGVzdDp0ZXN0")) {
            var response=request("GET","/api/fulfillments/"+UUID.randomUUID(),authorization,null);
            assertProblem(response,401);
            assertThat(response.headers().firstValue("WWW-Authenticate").orElseThrow()).isEqualTo("Bearer");
        }
    }

    @Test void duplicateBearerHeadersAreGenericBadRequests() throws Exception {
        var token=TestJwtConfiguration.operator();
        var builder=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/api/fulfillments/"+UUID.randomUUID()))
                .timeout(Duration.ofSeconds(10)).header("Authorization","Bearer "+token)
                .header("Authorization","Bearer "+token).GET();
        try(var client=HttpClient.newHttpClient()) {
            var response=client.send(builder.build(),HttpResponse.BodyHandlers.ofString());
            assertProblem(response,400);
            assertThat(response.headers().firstValue("WWW-Authenticate").orElseThrow())
                    .isEqualTo("Bearer error=\"invalid_request\"");
            assertThat(response.body()).doesNotContain(token,"Multiple","Authorization");
        }
    }

    @ParameterizedTest @ValueSource(strings={"malformed","unsigned","signature","expired","future","issuer","audience","no-audience","no-expiration"})
    void invalidCredentialsAreGeneric401(String kind) throws Exception {
        String token;
        switch(kind) {
            case "malformed" -> token="definitely-not-a-jwt";
            case "unsigned" -> token=new PlainJWT(new JWTClaimsSet.Builder().issuer(TestJwtConfiguration.ISSUER).audience(TestJwtConfiguration.AUDIENCE).build()).serialize();
            case "signature" -> token=TestJwtConfiguration.sign(new JWTClaimsSet.Builder().issuer(TestJwtConfiguration.ISSUER).audience(TestJwtConfiguration.AUDIENCE).expirationTime(Date.from(Instant.now().plusSeconds(600))).build(),TestJwtConfiguration.key());
            default -> token=TestJwtConfiguration.token("WAREXIS_OPERATOR", claims -> {
                switch(kind) {
                    case "expired" -> claims.expirationTime(Date.from(Instant.now().minusSeconds(300)));
                    case "future" -> claims.notBeforeTime(Date.from(Instant.now().plusSeconds(300)));
                    case "issuer" -> claims.issuer("https://untrusted.example.test");
                    case "audience" -> claims.audience("another-api");
                    case "no-audience" -> claims.audience((List<String>)null);
                    case "no-expiration" -> claims.expirationTime(null);
                    default -> throw new IllegalArgumentException();
                }
            });
        }
        var response=request("GET","/api/fulfillments/"+UUID.randomUUID(),"Bearer "+token,null);
        assertProblem(response,401);
        assertThat(response.headers().firstValue("WWW-Authenticate").orElseThrow()).isEqualTo("Bearer error=\"invalid_token\"");
        assertThat(response.body()).doesNotContain(token,"expired","issuer","audience","signature");
    }

    @Test void viewerReadsBothRoutesButCannotDispatch() throws Exception {
        var input=FulfillmentServiceTests.input();service.accept(input);var plan=service.byOrder(input.orderId());
        var bearer="Bearer "+TestJwtConfiguration.token("WAREXIS_VIEWER", claims -> {});
        assertThat(request("GET","/api/fulfillments/"+plan.id(),bearer,null).statusCode()).isEqualTo(200);
        assertThat(request("GET","/api/fulfillments/orders/"+input.orderId(),bearer,null).statusCode()).isEqualTo(200);
        var denied=request("POST","/api/fulfillments/"+plan.id()+"/ship",bearer,"{\"shipmentReference\":\"VIEWER-DENIED\"}");
        assertProblem(denied,403);
        assertThat(denied.headers().firstValue("WWW-Authenticate").orElseThrow()).contains("Bearer", "insufficient_scope");
        assertThat(service.get(plan.id())).isEqualTo(plan);
        assertThat(facts(plan.id())).isZero();
    }

    @Test void operatorReadsAndRepeatedAuthorizedDispatchPreservesOriginSnapshotAndOneFact() throws Exception {
        var input=FulfillmentServiceTests.input();service.accept(input);var plan=service.byOrder(input.orderId());
        var bearer="Bearer "+TestJwtConfiguration.operator();
        var body="{\"shipmentReference\":\"SECURITY-"+UUID.randomUUID()+"\"}";
        var first=request("POST","/api/fulfillments/"+plan.id()+"/ship",bearer,body);
        var second=request("POST","/api/fulfillments/"+plan.id()+"/ship",bearer,body);
        assertThat(first.statusCode()).isEqualTo(200);assertThat(second.body()).isEqualTo(first.body());
        assertThat(request("GET","/api/fulfillments/"+plan.id(),bearer,null).body()).isEqualTo(first.body());
        assertThat(request("GET","/api/fulfillments/orders/"+input.orderId(),bearer,null).statusCode()).isEqualTo(200);
        var shipped=service.get(plan.id());
        assertThat(shipped.confirmationEventId()).isEqualTo(input.eventId());assertThat(shipped.lines()).isEqualTo(input.lines());
        assertThat(shipped.createdAt()).isEqualTo(plan.createdAt());assertThat(facts(plan.id())).isEqualTo(1);
        assertStateless(first);assertStateless(second);
    }

    @ParameterizedTest @ValueSource(strings={"scope","client","prefixed","lowercase","malformed-realm","malformed-roles","mixed-unrecognized"})
    void unrelatedClaimsAndLookalikeRolesCannotGrantAccess(String kind) throws Exception {
        var token=TestJwtConfiguration.token("unrecognized", claims -> {
            switch(kind) {
                case "scope" -> claims.claim("scope","WAREXIS_OPERATOR WAREXIS_VIEWER");
                case "client" -> claims.claim("resource_access",Map.of("warexis-api",Map.of("roles",List.of("WAREXIS_OPERATOR"))));
                case "prefixed" -> claims.claim("realm_access",Map.of("roles",List.of("ROLE_WAREXIS_OPERATOR")));
                case "lowercase" -> claims.claim("realm_access",Map.of("roles",List.of("warexis_operator")));
                case "malformed-realm" -> claims.claim("realm_access","WAREXIS_OPERATOR");
                case "malformed-roles" -> claims.claim("realm_access",Map.of("roles","WAREXIS_OPERATOR"));
                case "mixed-unrecognized" -> claims.claim("realm_access",Map.of("roles",List.of(7,true,"OTHER")));
                default -> throw new IllegalArgumentException();
            }
        });
        assertProblem(request("GET","/api/fulfillments/"+UUID.randomUUID(),"Bearer "+token,null),403);
    }

    @ParameterizedTest @ValueSource(strings={"/actuator","/actuator/env","/actuator/health/db","/error","/login","/logout","/unknown","/.well-known/oauth-protected-resource"})
    void undefinedEndpointsAreDeniedEvenToOperator(String path) throws Exception {
        assertProblem(request("GET",path,null,null),401);
        assertProblem(request("GET",path,"Bearer "+TestJwtConfiguration.operator(),null),403);
    }

    @ParameterizedTest @ValueSource(strings={"HEAD","OPTIONS","PUT","DELETE","PATCH"})
    void unclassifiedMethodsAreDenied(String method) throws Exception {
        var response=request(method,"/api/fulfillments/"+UUID.randomUUID(),"Bearer "+TestJwtConfiguration.operator(),null);
        assertThat(response.statusCode()).isEqualTo(403);assertStateless(response);
    }

    @Test void requiredExternalConfigurationRejectsBlanks() {
        assertThatIllegalArgumentException().isThrownBy(() -> new SecurityConfiguration(" ","warexis-api"));
        assertThatIllegalArgumentException().isThrownBy(() -> new SecurityConfiguration(TestJwtConfiguration.ISSUER," "));
    }

    @Test void frameworkTimestampValidationAloneAllowsMissingExpirationButApplicationRejectsIt() {
        var jwt=Jwt.withTokenValue("test-only").header("alg","RS256").subject("subject").build();
        assertThat(new JwtTimestampValidator().validate(jwt).hasErrors()).isFalse();
        // The authenticated HTTP missing-expiration case verifies the additional production validator.
    }

    private long facts(UUID id) { return jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id=:id").param("id",id).query(Long.class).single(); }
    private HttpResponse<String> request(String method,String path,String authorization,String body) throws Exception {
        var builder=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+path)).timeout(Duration.ofSeconds(10));
        if(authorization!=null) builder.header("Authorization",authorization);
        if(body!=null) builder.header("Content-Type","application/json");
        builder.method(method,body==null?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofString(body));
        try(var client=HttpClient.newHttpClient()) { return client.send(builder.build(),HttpResponse.BodyHandlers.ofString()); }
    }
    private void assertProblem(HttpResponse<String> response,int status) {
        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(response.headers().firstValue("Content-Type").orElseThrow()).startsWith("application/problem+json");
        assertThat(response.body()).contains("\"status\":"+status).doesNotContain("Exception","JWT","stackTrace");
        assertStateless(response);
    }
    private void assertStateless(HttpResponse<String> response) {
        response.headers().firstValue("WWW-Authenticate").ifPresent(header -> assertThat(header).doesNotContain("resource_metadata"));
        assertThat(response.headers().allValues("Set-Cookie")).isEmpty();assertThat(response.headers().firstValue("Location")).isEmpty();
    }
}
