package com.nosqlcode.redjava;

import java.lang.annotation.*;

/**
 * User: thomas
 * Date: 10/9/13
 * Time: 10:29 PM
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface RedStr {
}
