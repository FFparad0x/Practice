





import com.practice.parser.Parser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;



public class TestParser {


    @Test  //not actual since function data type was changed
    public void testDate() {
        String date = "26-01-2021 21:10:43";
        Parser parser = new Parser();
        Assertions.assertEquals("2021-01-26 21:10:43", parser.convertTimestamp(date).toString());
    }

}