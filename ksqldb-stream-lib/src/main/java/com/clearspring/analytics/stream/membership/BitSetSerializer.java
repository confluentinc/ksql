/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.clearspring.analytics.stream.membership;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.BitSet;

public class BitSetSerializer {

    public static void serialize(BitSet bs, DataOutputStream dos) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(dos);
        oos.writeObject(bs);
        oos.flush();
    }

    public static BitSet deserialize(DataInputStream dis) throws IOException {
        ObjectInputStream ois = new ObjectInputStream(dis);
        try {
            return (BitSet) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
