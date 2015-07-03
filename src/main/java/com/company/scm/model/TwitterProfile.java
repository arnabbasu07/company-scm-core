package com.company.scm.model;

import java.io.Serializable;
import java.util.List;

public class TwitterProfile implements Serializable{

	private static final long serialVersionUID = 1L;
	private int employeeId;
	private String name;
	private String department;
	private String gender;
	private String designation;
	private String location;
	private String emailId;
	private List<String> languages;
	private String timezone;
	private List<String> friendlist;
	private Long twitterId;
	public int getEmployeeId() {
		return employeeId;
	}
	public void setEmployeeId(int employeeId) {
		this.employeeId = employeeId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDepartment() {
		return department;
	}
	public void setDepartment(String department) {
		this.department = department;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getDesignation() {
		return designation;
	}
	public void setDesignation(String designation) {
		this.designation = designation;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getEmailId() {
		return emailId;
	}
	public void setEmailId(String emailId) {
		this.emailId = emailId;
	}
	public List<String> getLanguages() {
		return languages;
	}
	public void setLanguages(List<String> languages) {
		this.languages = languages;
	}
	public List<String> getFriendlist() {
		return friendlist;
	}
	public void setFriendlist(List<String> friendlist) {
		this.friendlist = friendlist;
	}
	public String getTimezone() {
		return timezone;
	}
	public void setTimezone(String timezone) {
		this.timezone = timezone;
	}
	
	public Long getTwitterId() {
		return twitterId;
	}
	public void setTwitterId(Long twitterId) {
		this.twitterId = twitterId;
	}
	@Override
	public String toString(){
		/**
		 * Implement to String
		 */
		return "";
	}

	@Override
	public boolean equals(Object o){

		return false;
	}
	@Override
	public int hashCode() {

		return 0;
	}
}
