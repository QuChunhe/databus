package databus.util;


/**
 * Created by Qu Chunhe on 2016-10-31.
 */
public class Benchmark {

    public Benchmark() {
        beginTime = System.nanoTime();
    }

    public String elapsedMsec(int digit) {
        return format(Long.toString(System.nanoTime() - beginTime), digit);
    }

    private String format(String elapsedTime, int digit) {
        StringBuilder builder = new StringBuilder(8);
        for (int i=7; i>elapsedTime.length(); i--) {
            builder.append('0');
            if (7 == i) {
                builder.append('.');
            }
        }
        for (int pos = 0, count=0; (pos<elapsedTime.length()) && (count<digit); pos++,count++ ){
            char a = elapsedTime.charAt(pos);
            builder.append(a);
            if ((elapsedTime.length()-pos) == 7) {
                builder.append('.');
            }
        }
        for (int i=elapsedTime.length()-6; i>digit; i-- ) {
            builder.append('0');
        }
        return builder.toString();
    }

    private final long beginTime;
}
