package io.kanal.runner.controller;

import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import java.net.URI;
import java.net.URISyntaxException;

@Controller
public class RedirectController {

    @Value("${micronaut.server.context-path:}")
    protected String basePath;
    @Get
    public HttpResponse<?> redirect() throws URISyntaxException {
        return HttpResponse.redirect(new URI(basePath + "/ui"));
    }
}
