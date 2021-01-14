# macrosheds style guide

**when in doubt, follow tidy style guide**

+ explicitly pass arguments where instructive/clarifying (e.g. first occurrence of call in a sequence of similar calls):

    ```
    myfunc(par1 = argx, par2 = argy)  
    myfunc(argz)
    ```

+ redundantly name parameters that will be passed up the callstack:

    ```
    outer_func(foo = bar){  
        baz <- foo + 1  
        inner_func(foo = baz) #keep "foo" as the param name  
    }
    ```

    + edit deviations from this rule where they are encountered, but do not seek them

+ find the happy medium of vertical sprawl:
    
    ```
    #too compact
    mutate(a = x + x, b = y + y, c = z + z) %>%

    #good
    mutate(
        a = x + x,
        b = y + y,
        c = z + z) %>%

    #whoa, nelly
    mutate(
        a = x + x,
        b = y + y,
        c = z + z
    ) %>%

    #if only one tidy operation, no need for a pipe
    select(df, a, b)
    ```

+ use comments only where variable names can't speak for themselves. Generally, function names should be verbs and other variables should be named with nouns:

    ```
    #do thing with stuff
    out <- generic_funcname(x)

    #the above is lame. the below is better:

    thinged_stuff <- do_thing(specific_stuff)
    ```

+ `return` must be used in all functions. even if it's just `return()`. our error handling scheme requires this.

+ no space needed before the curly brace that sets off a function definition:

    `myfunc <- function(){`

#unprocessed stuff
    ', not "
    multiarg lines in shiny and when repetitive
    spaces around arithmetic and boolean operators
    test all hbef products with get_avail_lter_product_sets
    looping through integers, use i, j, k
    looping through vaues, use something more descriptive
    one_line pipers: filter(DF, operation)

