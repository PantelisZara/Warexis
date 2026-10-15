package io.github.panteliszara.warexis.fulfillment.configuration;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpHeaders;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.server.resource.BearerTokenError;
import org.springframework.security.oauth2.server.resource.BearerTokenErrors;
import org.springframework.security.oauth2.server.resource.web.BearerTokenResolver;
import org.springframework.security.oauth2.server.resource.web.DefaultBearerTokenResolver;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationEntryPoint;
import org.springframework.security.oauth2.server.resource.web.access.BearerTokenAccessDeniedHandler;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.DefaultSecurityFilterChain;
import org.springframework.security.oauth2.server.resource.web.OAuth2ProtectedResourceMetadataFilter;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.http.HttpStatus;
import org.springframework.util.Assert;

@Configuration(proxyBeanMethods = false)
public class SecurityConfiguration {
    public SecurityConfiguration(@Value("${WAREXIS_OAUTH2_ISSUER_URI}") String issuer,
            @Value("${WAREXIS_OAUTH2_AUDIENCE}") String audience) {
        Assert.hasText(issuer, "WAREXIS_OAUTH2_ISSUER_URI is required");
        Assert.hasText(audience, "WAREXIS_OAUTH2_AUDIENCE is required");
    }

    @Bean
    SecurityFilterChain apiSecurity(HttpSecurity http) throws Exception {
        var entryPoint = authenticationFailure();
        var deniedHandler = authorizationFailure();
        var chain = http
                // Explicit Bearer headers, rather than automatically attached cookie credentials.
                .csrf(AbstractHttpConfigurer::disable)
                .cors(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)
                .logout(AbstractHttpConfigurer::disable)
                .requestCache(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(rules -> rules
                        .dispatcherTypeMatchers(DispatcherType.ERROR).permitAll()
                        .requestMatchers(HttpMethod.GET, "/actuator/health").permitAll()
                        .requestMatchers(HttpMethod.GET, "/actuator/prometheus").hasRole("WAREXIS_METRICS")
                        .requestMatchers(HttpMethod.GET, "/api/fulfillments/{id}", "/api/fulfillments/orders/{orderId}")
                            .hasAnyRole("WAREXIS_VIEWER", "WAREXIS_OPERATOR")
                        .requestMatchers(HttpMethod.POST, "/api/fulfillments/{id}/ship").hasRole("WAREXIS_OPERATOR")
                        .anyRequest().denyAll())
                .exceptionHandling(errors -> errors.authenticationEntryPoint(entryPoint).accessDeniedHandler(deniedHandler))
                .oauth2ResourceServer(resource -> resource
                        .bearerTokenResolver(bearerTokenResolver())
                        .authenticationEntryPoint(entryPoint).accessDeniedHandler(deniedHandler)
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(authenticationConverter())))
                .build();
        // Security 7.1 adds a public discovery filter before authorization. This API
        // deliberately exposes only the classified routes, so keep discovery denied.
        return new DefaultSecurityFilterChain(chain.getRequestMatcher(), chain.getFilters().stream()
                .filter(filter -> !(filter instanceof OAuth2ProtectedResourceMetadataFilter)).toList());
    }

    private static BearerTokenResolver bearerTokenResolver() {
        var delegate = new DefaultBearerTokenResolver();
        return request -> {
            // Reject ambiguous credentials rather than silently selecting one.
            if (Collections.list(request.getHeaders(HttpHeaders.AUTHORIZATION)).size() > 1) {
                throw new OAuth2AuthenticationException(
                        BearerTokenErrors.invalidRequest("The authentication request is invalid."));
            }
            return delegate.resolve(request);
        };
    }

    @Bean
    OAuth2TokenValidator<Jwt> requiredExpiration() {
        // Boot adds this to its signature, issuer, audience and timestamp validation.
        return new JwtClaimValidator<Instant>("exp", Objects::nonNull);
    }

    static JwtAuthenticationConverter authenticationConverter() {
        var converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(jwt -> {
            Object realm = jwt.getClaims().get("realm_access");
            if (!(realm instanceof Map<?, ?> claims) || !(claims.get("roles") instanceof Collection<?> roles)) {
                return List.of();
            }
            return roles.stream().filter(role -> "WAREXIS_VIEWER".equals(role) || "WAREXIS_OPERATOR".equals(role) || "WAREXIS_METRICS".equals(role))
                    .<org.springframework.security.core.GrantedAuthority>map(role -> new SimpleGrantedAuthority("ROLE_" + role)).distinct().toList();
        });
        return converter;
    }

    private static AuthenticationEntryPoint authenticationFailure() {
        var bearer = new BearerTokenAuthenticationEntryPoint();
        bearer.setResourceMetadataParameterResolver(request -> null);
        return (request, response, failure) -> {
            var status = HttpStatus.UNAUTHORIZED;
            var sanitized = failure;
            if (failure instanceof OAuth2AuthenticationException oauth) {
                // Preserve the framework's invalid_request 400 / invalid_token 401
                // distinction without exposing credential or decoder diagnostics.
                var error = oauth.getError();
                if (error instanceof BearerTokenError bearerError) {
                    status = bearerError.getHttpStatus();
                }
                sanitized = new OAuth2AuthenticationException(
                        new BearerTokenError(error.getErrorCode(), status, null, null));
            }
            bearer.commence(request, response, sanitized);
            // Security 7.1 serializes a null metadata resolver result as a literal
            // parameter. Remove only that unused discovery parameter, retaining
            // the framework's standard Bearer challenge and error fields.
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE,
                    response.getHeader(HttpHeaders.WWW_AUTHENTICATE)
                            .replace(", resource_metadata=\"null\"", "")
                            .replace(" resource_metadata=\"null\"", ""));
            if (status == HttpStatus.BAD_REQUEST) {
                problem(response, 400, "Bad Request", "The authentication request is invalid.");
            } else {
                problem(response, 401, "Unauthorized", "Authentication is required.");
            }
        };
    }

    private static AccessDeniedHandler authorizationFailure() {
        var bearer = new BearerTokenAccessDeniedHandler();
        return (request, response, failure) -> {
            bearer.handle(request, response, failure);
            problem(response, 403, "Forbidden", "Access is denied.");
        };
    }

    private static void problem(HttpServletResponse response, int status, String title, String detail)
            throws java.io.IOException {
        response.setContentType("application/problem+json");
        response.getWriter().write("{\"type\":\"about:blank\",\"title\":\"" + title
                + "\",\"status\":" + status + ",\"detail\":\"" + detail + "\"}");
    }
}
