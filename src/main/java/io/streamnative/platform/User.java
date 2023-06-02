package io.streamnative.platform;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Builder
@AllArgsConstructor
@ToString
@NoArgsConstructor
public class User {
    String name;
    long id;
}
