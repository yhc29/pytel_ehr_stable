# Ensemble Fully Connected Fractional Cascading for discrete events

class EFCFCd:
	def __init__(self, subject_id, events, time_type="int"):
		self.subject_id = subject_id
		self.time_type = time_type
		self.fcs, self.date_dict, self.date_list = self.build_efcfcd(events)


	def build_efcfcd(self, events):
		date_dict = {} # key: date, value: {id: index of the date, events: list of events}
		event_dict = {} # key: event, value: set of dates containing the event
		fcs = {} # key: event, value: {fc: list of indices, date_indices: list of indices}
		for date, event in events:
			date = int(date) if self.time_type == "int" else date
			event = int(event)
			if date_dict.get(date) is None:
				date_dict[date] = {"id": None, "events": set()}
			if event_dict.get(event) is None:
				event_dict[event] = set()
			date_dict[date]["events"].add(event)
			event_dict[event].add(date)
		
		date_list = []
		for index, date in enumerate(sorted(date_dict.keys())):
			date_dict[date]["id"] = index
			date_list.append(date)
		
		# build fc for each event
		for event in event_dict:
			event_dates = sorted(list(event_dict[event]))
			fc_indices, fc_dates, date_indices = build_fc(event_dates, date_dict)
			fcs[event] = {"fc_indices": fc_indices, "fc_dates": fc_dates, "date_indices": date_indices, "dates": event_dates}
		return fcs, date_dict, date_list


def build_fc(event_dates, date_dict):
	fc_indices = []
	fc_dates = []
	date_indices = [date_dict[date]["id"] for date in event_dates]
	j = 0
	for i in range(len(date_dict)):
		if j >= len(date_indices):
			fc_indices.append(-1)
			fc_dates.append(None)
		elif i < date_indices[j]:
			fc_indices.append(date_indices[j])
			fc_dates.append(event_dates[j])
		elif i == date_indices[j]:
			fc_indices.append(date_indices[j])
			fc_dates.append(event_dates[j])
			j += 1
	return fc_indices, fc_dates, date_indices
		