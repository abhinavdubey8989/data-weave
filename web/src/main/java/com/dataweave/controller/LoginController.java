package com.dataweave.controller;

import com.dataweave.dto.request.LoginRequest;
import com.dataweave.service.LoginService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/api/v1/login")
@RequiredArgsConstructor
public class LoginController {

    @Autowired
    private LoginService loginService;

    @PostMapping("")
    public Object createLogin(
            @Valid @RequestBody LoginRequest req) {
        return this.loginService.login(req);
    }

}
