package io.github.panteliszara.warexis.order.configuration;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.BearerTokenErrors;
import org.springframework.security.oauth2.server.resource.web.DefaultBearerTokenResolver;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.DefaultSecurityFilterChain;
import org.springframework.security.oauth2.server.resource.web.OAuth2ProtectedResourceMetadataFilter;
import org.springframework.util.Assert;

@Configuration(proxyBeanMethods = false)
public class SecurityConfiguration {

    @Bean
    public OAuth2TokenValidator<Jwt> requiredTimeClaimsValidator(
            @Value("${spring.security.oauth2.resourceserver.jwt.issuer-uri}") String issuer,
            @Value("${spring.security.oauth2.resourceserver.jwt.audiences}") String audience) {
        Assert.hasText(issuer, "WAREXIS_OAUTH2_ISSUER_URI is required");
        Assert.hasText(audience, "WAREXIS_OAUTH2_AUDIENCE is required");
        // Boot combines this additional validator with its issuer/audience validators.
        // Security's default timestamp validator permits a missing exp claim.
        var timestamp = new JwtTimestampValidator();
        timestamp.setAllowEmptyExpiryClaim(false);
        return timestamp;
    }

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        var bearerResolver = new DefaultBearerTokenResolver();
        var converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(SecurityConfiguration::realmAuthorities);
        http.sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .requestCache(cache -> cache.disable())
                // Bearer credentials are supplied explicitly, never through authentication cookies.
                .csrf(csrf -> csrf.disable())
                .cors(cors -> cors.disable())
                .httpBasic(basic -> basic.disable())
                .formLogin(form -> form.disable())
                .logout(logout -> logout.disable())
                .authorizeHttpRequests(authorize -> authorize
                        .dispatcherTypeMatchers(DispatcherType.ERROR).permitAll()
                        .requestMatchers(HttpMethod.GET, "/actuator/health").permitAll()
                        .requestMatchers(HttpMethod.POST, "/api/orders").hasRole("WAREXIS_OPERATOR")
                        .requestMatchers(HttpMethod.GET, "/api/orders/{id}")
                            .hasAnyRole("WAREXIS_VIEWER", "WAREXIS_OPERATOR")
                        .anyRequest().denyAll())
                .exceptionHandling(errors -> errors
                        .authenticationEntryPoint((request, response, exception) -> unauthorized(response, exception))
                        .accessDeniedHandler((request, response, exception) -> forbidden(response)))
                .oauth2ResourceServer(resource -> resource
                        .bearerTokenResolver(request -> {
                            if (Collections.list(request.getHeaders(HttpHeaders.AUTHORIZATION)).size() > 1) {
                                throw new OAuth2AuthenticationException(
                                        BearerTokenErrors.invalidRequest("The request is invalid."));
                            }
                            return bearerResolver.resolve(request);
                        })
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(converter))
                        .authenticationEntryPoint((request, response, exception) -> unauthorized(response, exception))
                        .accessDeniedHandler((request, response, exception) -> forbidden(response)));
        var chain = http.build();
        // Security 7.1 installs this public metadata endpoint before authorization.
        // This API exposes only the explicitly classified health and business routes.
        return new DefaultSecurityFilterChain(chain.getRequestMatcher(), chain.getFilters().stream()
                .filter(filter -> !(filter instanceof OAuth2ProtectedResourceMetadataFilter)).toList());
    }

    static Collection<GrantedAuthority> realmAuthorities(Jwt jwt) {
        Object realm = jwt.getClaims().get("realm_access");
        if (!(realm instanceof Map<?, ?> access) || !(access.get("roles") instanceof Collection<?> roles)) {
            return List.of();
        }
        return roles.stream()
                .filter(role -> "WAREXIS_VIEWER".equals(role) || "WAREXIS_OPERATOR".equals(role))
                .distinct().map(role -> (GrantedAuthority) new SimpleGrantedAuthority("ROLE_" + role)).toList();
    }

    private static void unauthorized(HttpServletResponse response, Exception exception) throws IOException {
        if (exception instanceof OAuth2AuthenticationException oauth
                && "invalid_request".equals(oauth.getError().getErrorCode())) {
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer error=\"invalid_request\"");
            problem(response, 400, "Bad Request", "The request is invalid.");
            return;
        }
        String challenge = exception instanceof OAuth2AuthenticationException
                ? "Bearer error=\"invalid_token\"" : "Bearer";
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, challenge);
        problem(response, 401, "Unauthorized", "Authentication is required.");
    }

    private static void forbidden(HttpServletResponse response) throws IOException {
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer error=\"insufficient_scope\"");
        problem(response, 403, "Forbidden", "Access is denied.");
    }

    private static void problem(HttpServletResponse response, int status, String title, String detail)
            throws IOException {
        response.setStatus(status);
        response.setContentType("application/problem+json");
        response.getWriter().write("{\"type\":\"about:blank\",\"title\":\"" + title
                + "\",\"status\":" + status + ",\"detail\":\"" + detail + "\"}");
    }
}
