package io.github.panteliszara.warexis.inventory.security;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

final class RealmRoleConverter implements Converter<Jwt, Collection<GrantedAuthority>> {
    @Override
    public Collection<GrantedAuthority> convert(Jwt jwt) {
        if (!(jwt.getClaims().get("realm_access") instanceof Map<?, ?> realm)
                || !(realm.get("roles") instanceof Collection<?> roles)) {
            return List.of();
        }
        return roles.stream().filter(String.class::isInstance).map(String.class::cast)
                .filter(role -> role.equals("WAREXIS_VIEWER") || role.equals("WAREXIS_OPERATOR") || role.equals("WAREXIS_METRICS"))
                .distinct().<GrantedAuthority>map(role -> new SimpleGrantedAuthority("ROLE_" + role)).toList();
    }
}
