package com.hailin.shrine.job.integrate.entity;

import lombok.*;

import java.util.HashMap;
import java.util.Map;
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class AlarmInfo {

	private String type;

	private String level;

	private String name;

	private String title;

	private String message;

	private Map<String, String> customFields = new HashMap<>();


	public void addCustomField(String key, String value) {
		customFields.put(key, value);
	}

	@Override
	public String toString() {
		return "AlarmInfo{" + "type='" + type + '\'' + ", level='" + level + '\'' + ", name='" + name + '\''
				+ ", title='" + title + '\'' + ", message='" + message + '\'' + ", addtionalInfo="
				+ customFields.toString() + '}';
	}

}
