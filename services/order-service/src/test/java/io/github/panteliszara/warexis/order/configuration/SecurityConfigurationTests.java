package io.github.panteliszara.warexis.order.configuration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
class SecurityConfigurationTests {
    @ParameterizedTest @NullAndEmptySource @ValueSource(strings = {" ", "\t"})
    void issuerIsRequired(String issuer) {
        assertThatIllegalArgumentException().isThrownBy(() -> new SecurityConfiguration().requiredTimeClaimsValidator(issuer, "warexis-api"));
    }
    @ParameterizedTest @NullAndEmptySource @ValueSource(strings = {" ", "\t"})
    void audienceIsRequired(String audience) {
        assertThatIllegalArgumentException().isThrownBy(() -> new SecurityConfiguration().requiredTimeClaimsValidator("https://issuer.example.test", audience));
    }
}
