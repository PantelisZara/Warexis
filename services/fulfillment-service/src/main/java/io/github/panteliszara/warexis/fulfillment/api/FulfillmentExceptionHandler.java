package io.github.panteliszara.warexis.fulfillment.api;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import io.github.panteliszara.warexis.fulfillment.application.FulfillmentNotFoundException;
import io.github.panteliszara.warexis.fulfillment.application.ShipmentConflictException;

@RestControllerAdvice
public class FulfillmentExceptionHandler extends ResponseEntityExceptionHandler {
    @ExceptionHandler(FulfillmentNotFoundException.class)
    public ProblemDetail missing() {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, "Fulfillment not found.");
    }

    @ExceptionHandler(ShipmentConflictException.class)
    public ProblemDetail conflict() {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, "Shipment conflicts with committed state.");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail invalid() {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Request is malformed.");
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail failure() {
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "Request could not be completed.");
    }

    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception exception, Object body,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        return super.handleExceptionInternal(exception,
                ProblemDetail.forStatusAndDetail(status, "Request could not be accepted."), headers, status, request);
    }
}
