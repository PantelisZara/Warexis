package io.github.panteliszara.warexis.inventory.security;

import java.io.IOException;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationEntryPoint;
import org.springframework.security.oauth2.server.resource.web.access.BearerTokenAccessDeniedHandler;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.DefaultSecurityFilterChain;
import org.springframework.security.oauth2.server.resource.web.OAuth2ProtectedResourceMetadataFilter;
import org.springframework.util.Assert;

@Configuration(proxyBeanMethods = false)
public class SecurityConfiguration {
    public SecurityConfiguration(@Value("${WAREXIS_OAUTH2_ISSUER_URI}") String issuer,
                                 @Value("${WAREXIS_OAUTH2_AUDIENCE}") String audience) {
        Assert.hasText(issuer, "OAuth2 issuer must be configured");
        Assert.hasText(audience, "OAuth2 audience must be configured");
    }

    @Bean
    OAuth2TokenValidator<Jwt> requiredExpiry() {
        var validator = new JwtTimestampValidator();
        validator.setAllowEmptyExpiryClaim(false);
        return validator;
    }

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        var converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(new RealmRoleConverter());
        var entryPoint = new BearerTokenAuthenticationEntryPoint();
        entryPoint.setResourceMetadataParameterResolver(request -> null);
        var deniedHandler = new BearerTokenAccessDeniedHandler();
        var bearerTokenResolver = new org.springframework.security.oauth2.server.resource.web.DefaultBearerTokenResolver();
        // Credentials are explicit Bearer headers; this API does not authenticate with cookies.
        http.csrf(AbstractHttpConfigurer::disable)
                .cors(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)
                .logout(AbstractHttpConfigurer::disable)
                .requestCache(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(requests -> requests
                        .dispatcherTypeMatchers(DispatcherType.ERROR).permitAll()
                        .requestMatchers(HttpMethod.GET, "/actuator/health").permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/stock-items/{sku}")
                            .hasAnyRole("WAREXIS_VIEWER", "WAREXIS_OPERATOR")
                        .requestMatchers(HttpMethod.POST, "/api/stock-items").hasRole("WAREXIS_OPERATOR")
                        .requestMatchers(HttpMethod.PUT, "/api/stock-items/{sku}").hasRole("WAREXIS_OPERATOR")
                        .anyRequest().denyAll())
                .oauth2ResourceServer(oauth -> oauth.bearerTokenResolver(request -> {
                            if (java.util.Collections.list(request.getHeaders(org.springframework.http.HttpHeaders.AUTHORIZATION)).size() > 1) {
                                throw new org.springframework.security.oauth2.core.OAuth2AuthenticationException(
                                        org.springframework.security.oauth2.server.resource.BearerTokenErrors.invalidRequest("The request is invalid."));
                            }
                            return bearerTokenResolver.resolve(request);
                        }).jwt(jwt -> jwt.jwtAuthenticationConverter(converter))
                        .authenticationEntryPoint((request, response, failure) -> {
                            // Preserve standard Bearer status/code without disclosing resolver or decoder diagnostics.
                            var bearerError = failure instanceof org.springframework.security.oauth2.core.OAuth2AuthenticationException oauthFailure
                                    && oauthFailure.getError() instanceof org.springframework.security.oauth2.server.resource.BearerTokenError error
                                    ? error : org.springframework.security.oauth2.server.resource.BearerTokenErrors.invalidToken("The request is invalid.");
                            entryPoint.commence(request, response,
                                    new org.springframework.security.oauth2.core.OAuth2AuthenticationException(
                                            new org.springframework.security.oauth2.server.resource.BearerTokenError(
                                                    bearerError.getErrorCode(), bearerError.getHttpStatus(),
                                                    "The request is invalid.", null)));
                            problem(response, bearerError.getHttpStatus().value());
                        })
                        .accessDeniedHandler((request, response, failure) -> {
                            deniedHandler.handle(request, response, failure);
                            problem(response, 403);
                        }))
                .exceptionHandling(errors -> errors
                        .authenticationEntryPoint((request, response, failure) -> {
                            entryPoint.commence(request, response, failure);
                            problem(response, 401);
                        })
                        .accessDeniedHandler((request, response, failure) -> {
                            deniedHandler.handle(request, response, failure);
                            problem(response, 403);
                        }));
        var chain = http.build();
        // Security 7.1 adds public discovery before authorization. This API exposes only classified routes.
        return new DefaultSecurityFilterChain(chain.getRequestMatcher(), chain.getFilters().stream()
                .filter(filter -> !(filter instanceof OAuth2ProtectedResourceMetadataFilter)).toList());
    }

    private static void problem(HttpServletResponse response, int status) throws IOException {
        if (status == 400 || status == 401) {
            // The framework always emits this optional parameter, even when its resolver returns null.
            var challenge = response.getHeader("WWW-Authenticate");
            if (challenge != null) {
                response.setHeader("WWW-Authenticate", challenge
                        .replace(", resource_metadata=\"null\"", "")
                        .replace(" resource_metadata=\"null\"", ""));
            }
        }
        response.setCharacterEncoding("UTF-8");
        response.setContentType("application/problem+json");
        response.getWriter().write(switch (status) {
            case 400 -> "{\"type\":\"about:blank\",\"title\":\"Bad Request\",\"status\":400,\"detail\":\"The request is invalid.\"}";
            case 401 -> "{\"type\":\"about:blank\",\"title\":\"Unauthorized\",\"status\":401,\"detail\":\"Authentication is required.\"}";
            default -> "{\"type\":\"about:blank\",\"title\":\"Forbidden\",\"status\":403,\"detail\":\"Access is denied.\"}";
        });
    }
}
