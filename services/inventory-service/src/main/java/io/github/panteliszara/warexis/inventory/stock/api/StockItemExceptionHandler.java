package io.github.panteliszara.warexis.inventory.stock.api;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import io.github.panteliszara.warexis.inventory.stock.application.StockItemNotFoundException;
import io.github.panteliszara.warexis.inventory.stock.domain.StockConflictException;

@RestControllerAdvice(assignableTypes = StockItemController.class)
public class StockItemExceptionHandler {

    @ExceptionHandler({MethodArgumentNotValidException.class, HandlerMethodValidationException.class,
            HttpMessageNotReadableException.class, IllegalArgumentException.class})
    public ProblemDetail invalidRequest() {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Invalid stock item request.");
    }

    @ExceptionHandler(StockItemNotFoundException.class)
    public ProblemDetail notFound() {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, "Stock item was not found.");
    }

    @ExceptionHandler(StockConflictException.class)
    public ProblemDetail conflict(StockConflictException exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, exception.getMessage());
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ProblemDetail persistenceConflict() {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT,
                "Stock item conflicts with an existing SKU or stock constraint.");
    }
}
