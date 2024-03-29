package udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class CouccurrenceKeyword extends EvalFunc<DataBag> {

	public static final String[] keywords_goals = { "education", "arts",
			"health care", "human services", "religion", "hospitals general",
			"health organizations", "community", "economic development",
			"environment", "performing arts", "protestant agencies  churches",
			"higher education", "association", "services", "university",
			"jewish federated giving programs", "youth development",
			"medical research", "catholic agencies  churches", "noneeducation",
			"shelter", "housing", "recreation", "centers", "wildlife",
			"financial aid", "animals", "children", "historical societies",
			"nonearts", "secondary school", "scholarships",
			"historic preservation", "youth meeting YWCA",
			"art museums institute", "college", "theater", "music",
			"disasters", "specialty hospitals youth", "orchestras",
			"community  foundations research", "civil", "medical school",
			"agriculture", "sports", "human rights", "salvation army",
			"boys clubs", "girls clubs public  libraries aging", "athletics",
			"public affairs", "food", "single organization support",
			"voluntarism", "cultural", "ethnic awareness", "media", "science",
			"philanthropy", "crime", "employment", "theological school",
			"residential", "custodial care", "mental health",
			"international affairs", "clinics", "crisis services", "safety",
			"american red cross", "early childhood education", "opera",
			"public education", "food banks", "arts education", "women",
			"homeless", "boy scouts", "child development",
			"international relief", "elementary school", "hospices",
			"law enforcement", "reproductive health", "mutual aid societies",
			"public policy", "general charitable giving", "law school",
			"nonehigher education", "nonenoneeducation", "parks",
			"land resources", "family planning", "community development",
			"violence prevention", "aids", "playgrounds",
			"noneperforming arts", "nursing school", "formal", "medical care",
			"film", "video", "general education", "business school",
			"nonehospitals general", "visual arts", "water resources",
			"performing arts centers", "fund raising",
			"community health systems", "botanical gardens", "dance",
			"television", "reading", "ballet", "domestic violence",
			"fund distribution", "eye diseases", "family services",
			"developmentally disabled", "radio", "heart  circulatory diseases",
			"agencies  churches", "training", "diabetes", "alliance",
			"centers  services", "fire prevention", "literature", "advocacy",
			"control", "specialized museums college community", "junior",
			"engineering school", "girl scouts", "alzheimers disease",
			"history museums", " substance abuse", "day care",
			"basic skills  GED  adult education", "literacy social sciences",
			"school programs", "archaeology", "eye research", "special",
			"diabetes research", "technology", "pediatrics",
			"print publishing", "public health", "equal rights",
			"volunteer services", "heart  circulatory research", "history",
			"patient services", "folk arts", "reform", "civil liberties",
			"big brothers", "hurricane katrina",
			"international economic development", "child abuse", "autism",
			"urban", "big sisters", "cancer", "senior continuing care",
			"management", "breast cancer", "foster care", "technical aid",
			"science museums", " multiple sclerosis", "adoption",
			"federated giving programs", "aids research", "treatment",
			"economics", "convalescent facility", "forests", "dental care",
			"artists services", "childrens museums disability",
			"food distribution", "baseball", "teacher school",
			"information services", "energy", "golf", "international exchange",
			"natural history  museums equestrianism", "expense aid",
			"minorities", "engineering", "leukemia", "nursing home",
			"business", "immigrants", "administration", "meals on wheels",
			"regulation", "ethics", "arts councils", "breast cancer research",
			"neighborhood centers", "students", "cystic fibrosis",
			"international democracy  civil society development",
			"museums ethnic", "plant conservation", "neuroscience research",
			"brain research", "home services", "government agencies",
			"architecture", "marine science", "prevention",
			"rural development", "arthritis", "garden clubs",
			"independent living", "special  libraries group home",
			"horticulture", "maritime", "business promotion", "linguistics",
			"photography", "special olympics", "postsecondary",
			"cerebral palsy", "pediatrics research", "water pollution",
			"water sports", "kidney diseases", "language", "mathematics",
			"aquariums", "LGBT LGBTQ alzheimers disease research",
			"marine museums", "aviation", "fraternities", "sororities",
			"victim aid", "private independent  foundations hearing centers",
			"infants", "small businesses", "muscular dystrophy",
			"parkinsons disease", "space", "speech", "brain disorders",
			"dental school", "libraries school",
			"religious federated giving programs", "autism research", "design",
			"ear", "lung diseases", "spirituality", "vocational education",
			"medicine", "music choral", "public libraries", " winter sports",
			"biomedicine", "end of life care", "racquet sports", "sanctuaries",
			"football", "historical activities", "sustainable programs",
			"farmlands", "physical fitness", "academies", "climate change",
			"neuroscience", "reproductive rights", "sculpture",
			"social entrepreneurship", "biomedicine research",
			"bird preserves", "global warming", "leukemia research",
			"multiple sclerosis research", "cemeteries", "financing", "groups",
			"kidney research", "music ensembles", "basketball",
			"burial services", "health sciences school", "painting",
			"body enrichment", "counseling", "geriatrics", "mind", "obesity",
			"alcoholism", "alumni groups", "diseases rare research",
			"philosophy", "vision screening", "fisheries", "insurance",
			"lung research", "nonenonehospitals general", "palliative care",
			"parent education", "parkinsons disease research", "prescriptions",
			"spine disorders research", "animal population control",
			"computer science", "emergency transport services",
			"community foundations", " optometry", "rescue", "search",
			"soccer", "spine disorders", "citizen participation",
			"civic centers", "international law", "law", "adaptive sports",
			"behavioral science", "charter schools",
			"cystic fibrosis research", "legal services", "pharmacy",
			"repairs", "right to life", "art history", "dropout prevention",
			"ESL programs", "ex offenders", "goodwill industries",
			"job counseling", "language foreign", "psychology",
			"secondary school reform", "skin disorders",
			"amyotrophic lateral sclerosis elementary", "hobby",
			"journalism school", "food services",
			"international conflict resolution", "neighborhood associations",
			"offenders", "public interest law", "community service clubs",
			"endangered species", "financial counseling",
			"international studies", "nose  throat diseases",
			"pregnancy centers", "sexual abuse", "war memorials",
			"air pollution", "fairs", "hospital", "burn centers", "chemistry",
			"diseases rare", "down syndrome", "festivals",
			"private operating  foundations genealogy", "olympics",
			"orthopedics research", "sociology", "arms control",
			"arthritis research", "art  music therapy", "grief",
			"international migration", "political science",
			"public health school", "refugee issues", "cemetery company",
			"international agricultural development", "museums sports",
			"physical therapy", "poverty studies", "anthropology",
			"blood supply", "family resources and services",
			"holistic medicine", "legal rights", "lupus", "lupus research",
			"public housing", "theater musical", "veterinary medicine",
			"waste management", "bereavement counseling", "citizenship",
			"home owners", "music composition", "pregnancy prevention",
			"surgery", "epilepsy", "learning disorders", "orthopedics",
			"physics", "prenatal care", "recycling", "skin disorders research",
			"theater playwriting", "cooperative", "floods",
			"nose  throat research", "rehabilitation", "residential care",
			"ALS research", "art conservation", "bioethics",
			"libraries academic", "specialized museums", " obstetrics",
			"organ", "population studies", "prostate cancer",
			"rape victim services", "single parents", "theology",
			"tissue banks", "asthma", "childrens rights",
			"commodity distribution", "gift distribution", "middle schools",
			"muscular dystrophy research", "automotive safety", "ceramic arts",
			"choreography", "cultural rights", "epilepsy research",
			"groceries on wheels", "livestock issues", "adult education",
			" natural history museums", " pharmacology",
			"prostate cancer research", "surgery research",
			"cerebral palsy research", "citizen coalitions", "immunology",
			"public libraries toxics", "continuing education", "depression",
			"drawing", "environmental health", "first amendment",
			"geriatrics research", "hemophilia research", "PTA groups",
			"retraining", "suicide", "womens clubs", "2004 tsunami",
			"archives", "astronomy", "burial association",
			"immunology research", "internet", "landscaping", "liver research",
			"national security", "engineering school", "rural areas",
			"visitors", "assistive technology", "asthma research", "botany",
			"communicable diseases", "convention bureau", "dispute resolution",
			"economic development" };

	public static final String[] keywords_values = { "local", "international",
			"new", "national", "medical", "Expensive", "public", "social",
			"global", "original", "major", "private", "charitable",
			"political", "young", "financial", "humanitarian", "independent",
			"annual", "various", "human", "poor", "high", "non-profit",
			"educational", "American", "large", "sustainable", "small",
			"economic", "different", "non-governmental", "rural", "basic",
			"regional", "natural", "free", "available", "legal", "active",
			"environmental", "early", "general", "reliable", "primary",
			"religious", "individual", "philanthropic", "long-term", "special",
			"effective", "French", "European", "technical", "significant",
			"Canadian", "professional", "recent", "physical", "possible",
			"animal", "Australian", "Christian", "administrative",
			"Democratic", "military", "corporate", "main", "nonprofit",
			"responsible", "critical", "cultural", "direct", "mental",
			"British", "personal", "dead", "particular", "sexual", "academic",
			"clean", "affordable", "good", "Other", "positive", "federal",
			"initiative", "neutral", "strategic", "successful", "civil",
			"essential", "inappropriate", "potential", "single", "wide",
			"African", "operational", "commercial", "comprehensive", "enable",
			"reproductive", "spiritual", "strong", "vulnerable", "Asian",
			"future", "agricultural", "following", "great", "necessary",
			"promotional", "scientific", "traditional", "domestic", "healthy",
			"innovative", "music", "receive", "appropriate", "epidemic",
			"low-income", "Make-A-Wish", "real", "ongoing", "outstanding",
			"vocational", "memorial", "fundamental", "internal", "Japanese",
			"large-scale", "practical", "equal", "Executive", "full-time",
			"maternal", "organizational", "fiscal", "illegal", "pediatric",
			"regular", "well-being", "black", "civic", "clinical", "grant",
			"infectious", "Italian", "joint", "notable", "overseas", "popular",
			"unique", "accountable", "clinic", "ethnic", "historical",
			"immediate", "institutional", "serious", "severe", "extensive",
			"mobile", "needy", "not-for-profit", "portal", "previous",
			"standard", "true", "women's", "broad", "community-based",
			"pharmaceutical", "psychological", "Turkish", "voluntary", "daily",
			"elementary", "fair", "female", "fund-raising", "hospital",
			"indigenous", "modern", "Seventh-day", "advanced", "creative",
			"crucial", "famous", "impoverished", "Indian", "Kala-Azar",
			"respective", "surgical", "useful", "vital", "adequate",
			"biological", "clear", "co-founder", "controversial", "electoral",
			"emotional", "formal", "homeless", "normal", "nutritional",
			"preventable", "short-term", "therapeutic", "accessible",
			"cooperative", "endemic", "English", "evangelical",
			"monthly Russian", "safe substantial", "transitional", "white",
			"autonomous", "birth", "Chinese", "collaborative", "contemporary",
			"diagnostic", "efficient", "Former", "intellectual", "intensive",
			"minimum", "presidential", "productive", "secondary",
			"alternative", "chronic", "decision-making", "disadvantaged",
			"diverse", "ecological", "influential", "light", "marginalized",
			"moral", "musical", "official", "relevant", "same-sex", "solar",
			"temporary", "traffic", "unreliable", "war-torn", "anonymous",
			"civilian", "conceptual", "conservative", "criminal", "dental",
			"developed", "difficult", "genetic", "industrialized",
			"instrumental", "journal", "missionary", "nationwide", "racial",
			"relief", "rights-based", "secular", "Soviet", "tropical",
			"widespread", "bad", "big biomedical", "broadcast", "Cambodian",
			"collective", "considerable", "consultative", "earth",
			"ecumenical", "eligible", "equitable", "established", "evil",
			"exceptional", "excessive", "extraordinary", "faith-based",
			"five-year", "geographic", "governmental", "inclusive", "integral",
			"interactive", "Jewish", "juvenile", "long-standing", "male",
			"meaningful", "multilateral", "nongovernmental", "one-to-one",
			"outside", "peaceful", "preventive", "radical", "rich",
			"self-published", "subsequent", "tax-exempt", "underprivileged",
			"weekly", "willing", "acute", "complementary", "credible",
			"diplomatic", "doctoral", "dramatic", "ethical", "evidence-based",
			"geographical", "German", "impartial", "inaugural", "interested",
			"measurable", "multi-year", "ocean", "pastoral", "prestigious",
			"provincial", "recreational", "sensitive", "topic", "artistic",
			"capacity-building", "competitive", "digital", "distinctive",
			"generous", "harmful", "healthcare", "holistic", "inadequate",
			"informal", "irrespective", "machine-translated", "monetary",
			"non-medical", "non-partisan", "permanent", "Polish", "pregnant",
			"prime", "principal", "profitable", "proper", "retail", "rigorous",
			"sanctuary", "School-Based", "so-called", "socioeconomic", "song",
			"stable", "subjective", "Swiss", "theological", "treat",
			"visceral", "visible", "water-borne", "weak", "worldwide",
			"yearly", "Afghan-Canadian", "aged", "alleged", "ambitious",
			"anti-bullying", "antimalarial", "anti-nuclear", "at-risk",
			"behavior", "behavioral", "bilateral", "complicated", "conscious",
			"constant", "critic", "developmental", "Director-General", "drive",
			"dynamic", "dysferlinopathy", "educate", "exclusive", "expansive",
			"facilitate", "giant", "hands-on", "hazardous", "high-need",
			"honorary", "hungry", "in-country", "in-kind", "interim",
			"leading", "logistical", "man-made", "manual", "molecular",
			"musician", "native", "nearby", "nutritious", "obvious",
			"organized", "partial", "part-time", "post-World", "preliminary",
			"Public/Private", "referral", "remarkable", "renewable",
			"Republican", "sad", "Scottish", "Secretary-General", "subject",
			"suitable", "supportive", "sure", "tax-deductible",
			"volunteer under-served", "unstable", "wealthy", "protestant",
			"jewish mormon", "catholic orthodox", "adverse", "A-League",
			"ancient", "apparent", "architectural", "Asia-Pacific", "athletic",
			"authoritarian", "award-winning", "bear", "beneficiary", "blatant",
			"bold", "brief", "capable", "captive", "cardiovascular", "careful",
			"classical", "communal", "concrete", "curative", "Current",
			"dangerous", "Diabetic", "dietary", "distinguished", "Dutch",
			"encyclopedic", "protein", "engaging", "ensemble entrepreneurial",
			"experimental", "field-based", "fish", "forced", "fresh",
			"gender-sensitive", "give", "goal", "graphic", "high-profile",
			"high-quality", "income-generating", "inevitable",
			"insecticide-treated", "interdisciplinary" };

	TupleFactory mTupleFactory = TupleFactory.getInstance();
	BagFactory mBagFactory = BagFactory.getInstance();

	@Override
	public DataBag exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() < 3)
			return null;
		try {
			String webpage = (String) tuple.get(2);

			if (webpage == null) {
				throw new IOException("This parameter should not be null");
			}

			// Returns a bag with all the words that have appeared at least one
			// in the webpage
			DataBag output = mBagFactory.newDefaultBag();

			for (String keyword : keywords_values) {
				if (webpage.indexOf(keyword) != -1) {
					output.add(mTupleFactory.newTuple(keyword));
				}
			}

			return output;
		} catch (Exception e) {
			log.error(e);
			throw new IOException("Caught exception processing input row ", e);
		}
	}
}
