public class CompareTo {
    private static final String a = "01";
    private static final String c = "03";
    private static final String j = "10";
    private static final String m1 = "2021-10-31";
    private static final String m2 = "2021-12-30";
    private static final String m3 = "2021-09-03";
    private static final String m4 = "2021-11-11";
    private static final String m5 = "2022-09-03";

    public static void main(String[] args) {
        System.out.println(a.compareTo(c) < 0);
        System.out.println(j.compareTo(c) < 0);
        System.out.println();
        System.out.println(m1.compareTo(m2) > 0);
        System.out.println(m4.compareTo(m1) > 0 && m4.compareTo(m2) < 0);
        System.out.println(m5.compareTo(m4) > 0);
        System.out.println(m3.compareTo(m1) < 0);
    }
}
