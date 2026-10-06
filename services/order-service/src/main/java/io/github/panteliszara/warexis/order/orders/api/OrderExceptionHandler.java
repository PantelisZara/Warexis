package io.github.panteliszara.warexis.order.orders.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.beans.TypeMismatchException;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import io.github.panteliszara.warexis.order.orders.application.OrderNotFoundException;

@RestControllerAdvice
public class OrderExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(OrderNotFoundException.class)
    public ProblemDetail notFound() {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, "Order not found.");
    }

    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception exception, Object body,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        if (exception instanceof MethodArgumentNotValidException) {
            body = ProblemDetail.forStatusAndDetail(status,
                    "Provide 1 to 100 lines with a valid SKU and a positive integer quantity.");
        } else if (exception instanceof HttpMessageNotReadableException || exception instanceof TypeMismatchException) {
            body = ProblemDetail.forStatusAndDetail(status, "Request body or order ID is malformed.");
        } else if (status.is5xxServerError()) {
            logger.error("Order request failed", exception);
            body = ProblemDetail.forStatusAndDetail(status, "The order request could not be completed.");
        }
        return super.handleExceptionInternal(exception, body, headers, status, request);
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail unexpectedFailure(Exception exception) {
        logger.error("Order request failed", exception);
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "The order request could not be completed.");
    }
}
