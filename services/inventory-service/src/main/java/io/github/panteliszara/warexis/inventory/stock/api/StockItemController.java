package io.github.panteliszara.warexis.inventory.stock.api;

import java.net.URI;
import java.util.UUID;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.github.panteliszara.warexis.inventory.stock.application.StockItemService;
import io.github.panteliszara.warexis.inventory.stock.domain.StockItem;

@RestController
@RequestMapping("/api/stock-items")
public class StockItemController {

    private final StockItemService service;

    public StockItemController(StockItemService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<StockItemResponse> create(@Valid @RequestBody CreateStockItemRequest request) {
        var item = service.create(request.sku(), request.onHand());
        return ResponseEntity.created(URI.create("/api/stock-items/" + item.getSku()))
                .body(StockItemResponse.from(item));
    }

    @GetMapping("/{sku}")
    public StockItemResponse get(@PathVariable @Pattern(regexp = StockItem.SKU_PATTERN) String sku) {
        return StockItemResponse.from(service.get(sku));
    }

    @PutMapping("/{sku}")
    public StockItemResponse update(@PathVariable @Pattern(regexp = StockItem.SKU_PATTERN) String sku,
                                    @Valid @RequestBody UpdateStockItemRequest request) {
        return StockItemResponse.from(service.update(sku, request.onHand()));
    }

    public record CreateStockItemRequest(@NotNull @Pattern(regexp = StockItem.SKU_PATTERN) String sku,
                                         @NotNull @PositiveOrZero Integer onHand) {
    }

    public record UpdateStockItemRequest(@NotNull @PositiveOrZero Integer onHand) {
    }

    public record StockItemResponse(UUID id, String sku, int onHand, int reserved, int available) {

        static StockItemResponse from(StockItem item) {
            return new StockItemResponse(item.getId(), item.getSku(), item.getOnHand(),
                    item.getReserved(), item.getAvailable());
        }
    }
}
