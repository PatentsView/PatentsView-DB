package lodi;

import org.junit.*;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

public class PatternReplacementsTest {

    @Test
    public void testApply() {
        PatternReplacements rep = 
            new PatternReplacements()
            .add("foo", "bar")
            .add(" x ")
            .add("dog", "cat");

        assertThat(rep.apply("hello, world"), is("hello, world"));
        assertThat(rep.apply("fooxfoo"), is("barxbar"));
        assertThat(rep.apply("foo x foo"), is("barbar"));
        assertThat(rep.apply("foodog"), is("barcat"));
    }
}
