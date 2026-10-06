package io.github.panteliszara.warexis.inventory.stock.persistence;

import java.util.Optional;
import java.util.UUID;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import io.github.panteliszara.warexis.inventory.stock.domain.StockItem;

public interface StockItemRepository extends JpaRepository<StockItem, UUID> {

    Optional<StockItem> findBySku(String sku);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select item from StockItem item where item.sku = :sku")
    Optional<StockItem> findBySkuForUpdate(@Param("sku") String sku);
}
