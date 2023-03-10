package com.linjinwu.controller;

import com.linjinwu.pojo.OlapParameter;
import com.linjinwu.service.OlapService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/olap")
public class OlapController {
    @Autowired
    private OlapService olapService;

    @PostMapping("/query")
    @ResponseBody
    public String olap(@RequestBody OlapParameter olapParameter){
        System.out.println("name is " + olapParameter.getName());
        System.out.println("id is " + olapParameter.getId());
        return olapService.queryData();
    }

    @GetMapping("/query2")
    @ResponseBody
    public String olap2(@RequestParam String name){
        //System.out.println("name is " + name);
        //System.out.println("id is " + olapParameter.getId());
        return olapService.queryData();
    }
}
