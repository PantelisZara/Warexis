package io.github.panteliszara.warexis.inventory.stock.api;

import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.cfg.CoercionAction;
import tools.jackson.databind.cfg.CoercionInputShape;

@Configuration(proxyBeanMethods = false)
public class StockJsonConfiguration {

    @Bean
    JsonMapperBuilderCustomizer rejectNonStringTextValues() {
        return builder -> builder.withCoercionConfig(String.class, config -> config
                .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail));
    }
}
