package com.hufudb.onedb.core.data;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import com.hufudb.onedb.rpc.OneDBCommon.HeaderProto;

public class Header {
  public static final Header EMPTY = new Header(ImmutableList.of());
  protected List<Field> fields;

  protected Header() {
    this.fields = new ArrayList<>();
  }

  protected Header(List<Field> fields) {
    this.fields = ImmutableList.copyOf(fields);
  }

  protected Header(HeaderProto proto) {
    this.fields = proto.getFieldList().stream().map(f -> Field.fromProto(f)).collect(Collectors.toList());
  }

  public HeaderProto toProto() {
    return HeaderProto.newBuilder().addAllField(fields.stream().map(f -> f.toProto()).collect(Collectors.toList())).build();
  }

  public static Header fromProto(HeaderProto proto) {
    return new Header(proto);
  }

  public static Header joinHeader(Header left, Header right) {
    return newBuilder().merge(left).merge(right).build();
  }

  public List<Field> getFields() {
    return fields;
  }

  public FieldType getType(int index) {
    return fields.get(index).type;
  }

  public List<FieldType> getTypeList() {
    ImmutableList.Builder<FieldType> types = ImmutableList.builder();
    fields.stream().forEach(field -> types.add(field.getType()));
    return types.build();
  }

  public int getTypeId(int index) {
    return fields.get(index).type.ordinal();
  }

  public String getName(int index) {
    return fields.get(index).name;
  }

  public Level getLevel(int index) {
    return fields.get(index).level;
  }

  public Field getField(int index) {
    Field ori = fields.get(index);
    return new Field(ori.name, ori.type, ori.level);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public int size() {
    return fields.size();
  }

  public Header immutableCopy() {
    return new Header(fields);
  }

  public static class Builder {
    private List<Field> fields;
    
    private Builder() {
      fields = new ArrayList<Field>();
    }

    public Builder add(String name, FieldType type) {
      fields.add(Field.of(name, type));
      return this;
    }

    public Builder add(String name, FieldType type, Level level) {
      fields.add(Field.of(name, type, level));
      return this;
    }

    public Builder add(Field field) {
      fields.add(field);
      return this;
    }

    public Builder merge(Header header) {
      fields.addAll(header.getFields());
      return this;
    }

    public Header build() {
      return new Header(fields);
    }

    public int size() {
      return fields.size();
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || (obj instanceof Header && fields.equals(((Header) obj).fields));
  }

  @Override
  public String toString() {
    List<String> columnStr = new ArrayList<>();
    for (Field field : fields) {
      columnStr.add(field.toString());
    }
    return String.join(" | ", columnStr);
  }
}
