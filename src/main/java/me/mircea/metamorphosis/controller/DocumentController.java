package me.mircea.metamorphosis.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.mircea.metamorphosis.service.WordCountService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/documents")
@RequiredArgsConstructor
@Slf4j
public class DocumentController {
    private final WordCountService wordCountService;

    @PostMapping
    public void sendDocument(@RequestBody String message) {
        wordCountService.ingestMessage(message);
    }

    @GetMapping("/count/{word}")
    public long findWordCount(@PathVariable String word) {
        return wordCountService.countAppearances(word);
    }
}
