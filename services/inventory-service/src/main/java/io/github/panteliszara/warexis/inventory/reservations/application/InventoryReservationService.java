package io.github.panteliszara.warexis.inventory.reservations.application;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.github.panteliszara.warexis.inventory.outbox.InventoryOutboxRepository;
import io.github.panteliszara.warexis.inventory.reservations.domain.InventoryReservation;
import io.github.panteliszara.warexis.inventory.reservations.domain.InventoryReservation.ReasonCode;
import io.github.panteliszara.warexis.inventory.reservations.domain.InventoryReservation.Status;
import io.github.panteliszara.warexis.inventory.reservations.persistence.InventoryReservationRepository;
import io.github.panteliszara.warexis.inventory.stock.domain.StockItem;
import io.github.panteliszara.warexis.inventory.stock.persistence.StockItemRepository;

@Service
public class InventoryReservationService {
    private final InventoryReservationRepository reservations;
    private final StockItemRepository stocks;
    private final InventoryOutboxRepository outbox;

    public InventoryReservationService(InventoryReservationRepository reservations,
                                       StockItemRepository stocks, InventoryOutboxRepository outbox) {
        this.reservations = reservations;
        this.stocks = stocks;
        this.outbox = outbox;
    }

    @Transactional
    public InventoryReservation process(OrderPlaced input) {
        reservations.lockInput(input.eventId(), input.orderId());
        var processed = reservations.findByEventId(input.eventId());
        if (processed.isPresent()) {
            return equivalent(processed.get(), input);
        }
        var existing = reservations.findByOrderId(input.orderId());
        if (existing.isPresent()) {
            var decision = equivalent(existing.get(), input);
            reservations.markProcessed(input.eventId(), decision);
            return decision;
        }

        // ASCII SKU ordering matches PostgreSQL C collation; repeated demand cannot overflow int.
        var demand = new TreeMap<String, Long>();
        input.lines().forEach(line -> demand.merge(line.sku(), (long) line.quantity(), Long::sum));
        var lockedStocks = new ArrayList<StockItem>();
        ReasonCode rejection = null;
        for (var entry : demand.entrySet()) {
            var stock = stocks.findBySkuForUpdate(entry.getKey());
            if (stock.isEmpty()) {
                rejection = ReasonCode.UNKNOWN_SKU;
            } else {
                lockedStocks.add(stock.get());
                if (entry.getValue() > stock.get().getAvailable() && rejection == null) {
                    rejection = ReasonCode.INSUFFICIENT_STOCK;
                }
            }
        }
        if (rejection == null) {
            lockedStocks.forEach(stock -> stock.reserve(demand.get(stock.getSku()).intValue()));
            stocks.flush();
        }
        var reservation = new InventoryReservation(UUID.randomUUID(), input.orderId(), input.eventId(),
                rejection == null ? Status.RESERVED : Status.REJECTED, rejection,
                Instant.now().truncatedTo(ChronoUnit.MICROS), input.lines());
        reservations.save(reservation);
        reservations.markProcessed(input.eventId(), reservation);
        outbox.append(reservation);
        return reservation;
    }

    private InventoryReservation equivalent(InventoryReservation existing, OrderPlaced input) {
        if (!existing.orderId().equals(input.orderId()) || !existing.lines().equals(input.lines())) {
            throw new ConflictingOrderPlacedException();
        }
        return existing;
    }
}
