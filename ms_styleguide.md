# macrosheds style guide

**when in doubt, follow tidy style guide**

And don't worry much about these first couple. The simple stylistic ones are below.

+ explicitly pass arguments where instructive/clarifying (e.g. first occurrence of call in a sequence of similar calls):

    ```
    myfunc(par1 = argx, par2 = argy) #more than one parameter. nice to clarify
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

+ no space needed before the curly brace that sets off a function definition:

    `myfunc <- function(){`

+ use TRUE and FALSE instead of T and F

+ use spaces around most operators, including !, ==, %in%, +, <-, =, >=, etc.

    spaces are not needed around indexing/subsetting/referencing operators like [, [[, $, and ::

+ use a space after the comma when indexing rows but not columns of a data.frame, e.g. df[1:3, ]

+ use single quotes rather than double quotes to denote strings. Only use double quotes if your string includes literal "'"

+ for loops over indices, use i as your loop counter. for nested loops, use j, then k, etc.
    
    + for loops over values, either use a single letter (other than i, j, k, l) that relates to the values being looped over, or use a short and descriptive word.

+ set Rstudio to "Strip trailing horizontal whitespace when saving" in tools > global options > saving

#unprocessed stuff
    multiarg lines in shiny and when repetitive
    test all hbef products with get_avail_lter_product_sets

