package io.github.panteliszara.warexis.inventory.stock.application;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.github.panteliszara.warexis.inventory.stock.domain.StockItem;
import io.github.panteliszara.warexis.inventory.stock.persistence.StockItemRepository;

@Service
public class StockItemService {

    private final StockItemRepository repository;

    public StockItemService(StockItemRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public StockItem create(String sku, int onHand) {
        return repository.saveAndFlush(StockItem.create(sku, onHand));
    }

    @Transactional(readOnly = true)
    public StockItem get(String sku) {
        return repository.findBySku(sku).orElseThrow(StockItemNotFoundException::new);
    }

    @Transactional
    public StockItem update(String sku, int onHand) {
        var item = repository.findBySkuForUpdate(sku).orElseThrow(StockItemNotFoundException::new);
        item.updateOnHand(onHand);
        repository.flush();
        return item;
    }
}
