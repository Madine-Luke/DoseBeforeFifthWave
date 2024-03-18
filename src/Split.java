public class Split {
    public static void main(String[] args) {
        String string = ",M,7,0";
        String[] strings = string.split(",");
        System.out.println(strings[0]);
        System.out.println(strings[1]);
        System.out.println(strings[2]);
        System.out.println(strings[3]);
        System.out.println("strings length:" + strings.length);
    }
}
