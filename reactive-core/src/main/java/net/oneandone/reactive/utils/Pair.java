package net.oneandone.reactive.utils;

import com.google.common.base.Objects;

public class Pair<F,S> {

    private final F first;
    private final S second;


    private Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    /**
     * @return the first item in the pair
     */
    public F getFirst() {
        return first;
    }
    
    /**
     * @return the second item in the pair
     */
    public S getSecond() {
        return second;
    }

    /**
     * @param first the first item to store in the pair
     * @param second the second item to store in the pair
     * @param <S> the type of the first item
     * @param <T> the type of the second item
     * @return a new pair wrapping the two items
     */
    public static <F, S> Pair<F, S> of(F first, S second) {
        return new Pair<>(first,second);
    }
    
    
    @Override
    public String toString() {
        return "[" + first + ", " + second + "]";
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(first, second);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object other) {
        return (other != null) &&
               (other instanceof Pair) &&
               ((Pair) other).first.equals(this.first) &&
               ((Pair) other).second.equals(this.second);
    }
}
