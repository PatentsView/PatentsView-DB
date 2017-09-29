package lodi;

import org.junit.*;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

public class RawLocationTest {

    @Test
    public void testConcatenateLocation() {
	    String s = RawLocation.concatenateLocation("washington", "dc", "us");
        assertThat(s, is("washington, dc, us"));

        s = RawLocation.concatenateLocation("", "dc", "us");
        assertThat(s, is("dc, us"));

        s = RawLocation.concatenateLocation("washington", null, "us");
        assertThat(s, is("washington, us"));
    }

    @Test
    public void quickFixText() {
        assertThat(RawLocation.quickFix("{blah blah (X)}"), is("X"));
        assertThat(RawLocation.quickFix("{blah (Y) bah}"), is("Y"));
        assertThat(RawLocation.quickFix("{(Z) blah buh}"), is("Z"));
        assertThat(RawLocation.quickFix("(Z) blah buh"), is("(Z) blah buh"));
    }
}
