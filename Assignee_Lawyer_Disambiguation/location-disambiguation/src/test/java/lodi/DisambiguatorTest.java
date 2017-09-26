package lodi;

import org.junit.*;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

import java.util.LinkedList;

public class DisambiguatorTest {

    RawLocation.Record raw1, raw2, raw3, rawForeign;
    Cities.Record city1, city2, city3, cityForeign;

    @Before
    public void setUp() {
        raw1 = new RawLocation.Record("1", "1", "Springfield", "IL", "US");
        raw2 = new RawLocation.Record("2", "1", "Springfield", "CA", "US");
        raw3 = new RawLocation.Record("3", "1", "Springfield", "SD", "US");
        rawForeign = new RawLocation.Record("4", "1", "Madrid", null, "ES");

        city1 = new Cities.Record(1, "Springfield", "IL", "US", 0.0, 0.0, null);
        city2 = new Cities.Record(1, "Springfield", "CA", "US", 0.0, 0.0, null);
        city3 = new Cities.Record(1, "Springfield", "SD", "US", 0.0, 0.0, null);
        cityForeign = new Cities.Record(1, "Madrid", null, "ES", 0.0, 0.0, null);

        raw1.linkedCity = city1;
        raw2.linkedCity = city2;
        raw3.linkedCity = city3;
        rawForeign.linkedCity = cityForeign;
    }

    @Test
    public void testNormalizeInventor() {
        LinkedList<RawLocation.Record> records = new LinkedList<>();

    }
}
